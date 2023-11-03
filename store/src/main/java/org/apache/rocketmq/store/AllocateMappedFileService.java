/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;

/**
 * https://blog.csdn.net/usagoole/article/details/126394486
 * https://www.cnblogs.com/tera/p/16035291.html
 * 异步创建mappedFile的服务
 * <p>
 * 提前创建MappedFile服务线程类,启动线程在DefaultMessageStore类中
 * AllocateMappedFileService用于提前创建一个MappedFile和下一个MappedFile
 * <p>
 * <p>
 * Create MappedFile in advance
 */
public class AllocateMappedFileService extends ServiceThread {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    //等待创建MappedFile的超时时间，默认5秒
    private static int waitTimeOut = 1000 * 5;

    //用来保存当前所有待处理的分配请求，其中key是filePath,value是分配请求AllocateRequest。
    //如果分配请求被成功处理，即获取到映射文件则从请求会从requestTable中移除
    private ConcurrentMap<String, AllocateRequest> requestTable =
            new ConcurrentHashMap<>();
    private PriorityBlockingQueue<AllocateRequest> requestQueue =
            new PriorityBlockingQueue<>();
    //创建MappedFile是否有异常

    private volatile boolean hasException = false;
    private DefaultMessageStore messageStore;

    public AllocateMappedFileService(DefaultMessageStore messageStore) {
        this.messageStore = messageStore;
    }

    /**
     * 异步创建 MappedFile
     * <p>
     * 因此对于AllocateMappedFileService.putRequestAndReturnMappedFile，主要工作也是2步：
     * 1.将“创建mappedFile”的请求放入队列中
     * 2.等待异步线程实际创建完mappedFile
     *
     * @param nextFilePath
     * @param nextNextFilePath
     * @param fileSize
     * @return
     */
    public MappedFile putRequestAndReturnMappedFile(String nextFilePath, String nextNextFilePath, int fileSize) {

        int canSubmitRequests = 2;

        if (this.messageStore.isTransientStorePoolEnable()) {
            if (this.messageStore.getMessageStoreConfig().isFastFailIfNoBufferInStorePool()
                    && BrokerRole.SLAVE != this.messageStore.getMessageStoreConfig().getBrokerRole()) { //if broker is slave, don't fast fail even no buffer in pool
                canSubmitRequests = this.messageStore.remainTransientStoreBufferNumbs() - this.requestQueue.size();
            }
        }
        //创建mappedFile的请求，
        AllocateRequest nextReq = new AllocateRequest(nextFilePath, fileSize);
        //将其放入ConcurrentHashMap中，主要用于并发判断，保证不会创建重复的mappedFile
        boolean nextPutOK = this.requestTable.putIfAbsent(nextFilePath, nextReq) == null;

        //如果map添加成功，就可以将request放入队列中，实际创建mappedFile的线程也是从该queue中获取request
        if (nextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so create mapped file error, " +
                        "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.remainTransientStoreBufferNumbs());
                this.requestTable.remove(nextFilePath);
                return null;
            }

            boolean offerOK = this.requestQueue.offer(nextReq);
            if (!offerOK) {
                log.warn("never expected here, add a request to preallocate queue failed");
            }
            canSubmitRequests--;
        }

        AllocateRequest nextNextReq = new AllocateRequest(nextNextFilePath, fileSize);
        boolean nextNextPutOK = this.requestTable.putIfAbsent(nextNextFilePath, nextNextReq) == null;
        if (nextNextPutOK) {
            if (canSubmitRequests <= 0) {
                log.warn("[NOTIFYME]TransientStorePool is not enough, so skip preallocate mapped file, " +
                        "RequestQueueSize : {}, StorePoolSize: {}", this.requestQueue.size(), this.messageStore.remainTransientStoreBufferNumbs());
                this.requestTable.remove(nextNextFilePath);
            } else {
                boolean offerOK = this.requestQueue.offer(nextNextReq);
                if (!offerOK) {
                    log.warn("never expected here, add a request to preallocate queue failed");
                }
            }
        }

        if (hasException) {
            log.warn(this.getServiceName() + " service has exception. so return null");
            return null;
        }

        AllocateRequest result = this.requestTable.get(nextFilePath);
        try {
            if (result != null) {
                messageStore.getPerfCounter().startTick("WAIT_MAPFILE_TIME_MS");
                //因为是异步创建，所以这里需要await，等待mappedFile被异步创建成功
                boolean waitOK = result.getCountDownLatch().await(waitTimeOut, TimeUnit.MILLISECONDS);
                messageStore.getPerfCounter().endTick("WAIT_MAPFILE_TIME_MS");
                if (!waitOK) {
                    log.warn("create mmap timeout " + result.getFilePath() + " " + result.getFileSize());
                    return null;
                } else {
                    this.requestTable.remove(nextFilePath);
                    //返回创建好的mappedFile
                    return result.getMappedFile();
                }
            } else {
                log.error("find preallocate mmap failed, this never happen");
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
        }

        return null;
    }

    /**
     * 获取服务名称
     *
     * @return
     */
    @Override
    public String getServiceName() {
        if (messageStore != null && messageStore.getBrokerConfig().isInBrokerContainer()) {
            return messageStore.getBrokerIdentity().getIdentifier() + AllocateMappedFileService.class.getSimpleName();
        }
        return AllocateMappedFileService.class.getSimpleName();
    }

    @Override
    public void shutdown() {
        super.shutdown(true);
        for (AllocateRequest req : this.requestTable.values()) {
            if (req.mappedFile != null) {
                log.info("delete pre allocated maped file, {}", req.mappedFile.getFileName());
                req.mappedFile.destroy(1000);
            }
        }
    }

    /**
     * 此线程在DefaultMessageStore创建时启动
     */
    public void run() {
        log.info(this.getServiceName() + " service started");
        // 是否关闭

        while (!this.isStopped() && this.mmapOperation()) {

        }
        log.info(this.getServiceName() + " service end");
    }

    /**
     * 此线程在DefaultMessageStore创建时启动
     * 仅被外部线程中断，将返回false
     * <p>
     * 因此对于mmapOperation创建mappedFile，主要工作为4步：
     * 1.从队列中获取putRequestAndReturnMappedFile方法存放的request
     * 2.根据是否启用对外内存，分支创建mappedFile
     * 3.预热mappedFile
     * 4.唤醒putRequestAndReturnMappedFile方法中的等待线程
     * <p>
     * Only interrupted by the external thread, will return false
     */
    private boolean mmapOperation() {
        boolean isSuccess = false;
        AllocateRequest req = null;
        try {
            //从队列中拿request
            req = this.requestQueue.take();
            AllocateRequest expectedRequest = this.requestTable.get(req.getFilePath());
            if (null == expectedRequest) {
                log.warn("this mmap request expired, maybe cause timeout " + req.getFilePath() + " "
                        + req.getFileSize());
                return true;
            }
            if (expectedRequest != req) {
                log.warn("never expected here,  maybe cause timeout " + req.getFilePath() + " "
                        + req.getFileSize() + ", req:" + req + ", expectedRequest:" + expectedRequest);
                return true;
            }

            if (req.getMappedFile() == null) {
                long beginTime = System.currentTimeMillis();

                MappedFile mappedFile;
                //如果启用了临时存储池
                //判断是否采用堆外内存
                if (messageStore.isTransientStorePoolEnable()) {
                    try {
                        //如果开启了堆外内存，rocketmq允许外部注入自定义的MappedFile实现
                        mappedFile = ServiceLoader.load(MappedFile.class).iterator().next();
                        mappedFile.init(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    } catch (RuntimeException e) {
                        //如果没有自定义实现，那么就采用默认的实现
                        log.warn("Use default implementation.");
                        mappedFile = new DefaultMappedFile(req.getFilePath(), req.getFileSize(), messageStore.getTransientStorePool());
                    }
                } else {
                    //如果未采用堆外内存，那么就直接采用默认实现
                    mappedFile = new DefaultMappedFile(req.getFilePath(), req.getFileSize());
                }

                long elapsedTime = UtilAll.computeElapsedTimeMilliseconds(beginTime);
                if (elapsedTime > 10) {
                    int queueSize = this.requestQueue.size();
                    log.warn("create mappedFile spent time(ms) " + elapsedTime + " queue size " + queueSize
                            + " " + req.getFilePath() + " " + req.getFileSize());
                }

                // pre write mappedFile
                if (mappedFile.getFileSize() >= this.messageStore.getMessageStoreConfig()
                        .getMappedFileSizeCommitLog()
                        &&
                        this.messageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
                    //这里会预热文件，这里涉及到了系统的底层调用
                    mappedFile.warmMappedFile(this.messageStore.getMessageStoreConfig().getFlushDiskType(),
                            this.messageStore.getMessageStoreConfig().getFlushLeastPagesWhenWarmMapedFile());
                }

                req.setMappedFile(mappedFile);
                this.hasException = false;
                isSuccess = true;
            }
        } catch (InterruptedException e) {
            log.warn(this.getServiceName() + " interrupted, possibly by shutdown.");
            this.hasException = true;
            return false;
        } catch (IOException e) {
            log.warn(this.getServiceName() + " service has exception. ", e);
            this.hasException = true;
            if (null != req) {
                requestQueue.offer(req);
                try {
                    Thread.sleep(1);
                } catch (InterruptedException ignored) {
                }
            }
        } finally {
            if (req != null && isSuccess)
                //无论是否创建成功，都要唤醒putRequestAndReturnMappedFile方法中的等待线程
                req.getCountDownLatch().countDown();
        }
        return true;
    }

    /**
     * 请求信息
     */
    static class AllocateRequest implements Comparable<AllocateRequest> {
        // Full file path
        private String filePath;
        private int fileSize;
        //为0表示MappedFile创建完成
        private CountDownLatch countDownLatch = new CountDownLatch(1);
        private volatile MappedFile mappedFile = null;

        public AllocateRequest(String filePath, int fileSize) {
            this.filePath = filePath;
            this.fileSize = fileSize;
        }

        public String getFilePath() {
            return filePath;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }

        public int getFileSize() {
            return fileSize;
        }

        public void setFileSize(int fileSize) {
            this.fileSize = fileSize;
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }

        public MappedFile getMappedFile() {
            return mappedFile;
        }

        public void setMappedFile(MappedFile mappedFile) {
            this.mappedFile = mappedFile;
        }
        /**
         * fileSize大的优先级高，文件大小相同，文件的offset越小优先级越高
         */
        public int compareTo(AllocateRequest other) {
            if (this.fileSize < other.fileSize)
                return 1;
            else if (this.fileSize > other.fileSize) {
                return -1;
            } else {
                int mIndex = this.filePath.lastIndexOf(File.separator);
                long mName = Long.parseLong(this.filePath.substring(mIndex + 1));
                int oIndex = other.filePath.lastIndexOf(File.separator);
                long oName = Long.parseLong(other.filePath.substring(oIndex + 1));
                if (mName < oName) {
                    return -1;
                } else if (mName > oName) {
                    return 1;
                } else {
                    return 0;
                }
            }
            // return this.fileSize < other.fileSize ? 1 : this.fileSize >
            // other.fileSize ? -1 : 0;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((filePath == null) ? 0 : filePath.hashCode());
            result = prime * result + fileSize;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AllocateRequest other = (AllocateRequest) obj;
            if (filePath == null) {
                if (other.filePath != null)
                    return false;
            } else if (!filePath.equals(other.filePath))
                return false;
            if (fileSize != other.fileSize)
                return false;
            return true;
        }
    }
}
