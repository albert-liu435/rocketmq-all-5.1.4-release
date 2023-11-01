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

package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.store.DefaultMessageStore;

/**
 * 与Slave紧密相关的HAClient
 *  前面我们说到了只有是Salve角色的Broker才会真正的配置Master的地址，而HAClient是需要Master地址的，因此这个类真正在运行的时候只有Slave才会真正的使用到
 */
public class DefaultHAClient extends ServiceThread implements HAClient {

    /**
     * Report header buffer size. Schema: slaveMaxOffset. Format:
     *
     * <pre>
     * ┌───────────────────────────────────────────────┐
     * │                  slaveMaxOffset               │
     * │                    (8bytes)                   │
     * ├───────────────────────────────────────────────┤
     * │                                               │
     * │                  Report Header                │
     * </pre>
     * <p>
     */
    public static final int REPORT_HEADER_SIZE = 8;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
    //master地址
    private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
    private final AtomicReference<String> masterAddress = new AtomicReference<>();
    //Slave向Master发起主从同步的拉取偏移量，固定8个字节
    private final ByteBuffer reportOffset = ByteBuffer.allocate(REPORT_HEADER_SIZE);
    private SocketChannel socketChannel;
    private Selector selector;
    /**
     * last time that slave reads date from master.
     */
    //上次同步偏移量的时间戳
    private long lastReadTimestamp = System.currentTimeMillis();
    /**
     * last time that slave reports offset to master.
     */
    private long lastWriteTimestamp = System.currentTimeMillis();

    //反馈Slave当前的复制进度，commitlog文件最大偏移量
    private long currentReportedOffset = 0;
    private int dispatchPosition = 0;
    //读缓冲大小
    private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    private DefaultMessageStore defaultMessageStore;
    private volatile HAConnectionState currentState = HAConnectionState.READY;
    private FlowMonitor flowMonitor;

    public DefaultHAClient(DefaultMessageStore defaultMessageStore) throws IOException {
        this.selector = NetworkUtil.openSelector();
        this.defaultMessageStore = defaultMessageStore;
        this.flowMonitor = new FlowMonitor(defaultMessageStore.getMessageStoreConfig());
    }

    public void updateHaMasterAddress(final String newAddr) {
        String currentAddr = this.masterHaAddress.get();
        if (masterHaAddress.compareAndSet(currentAddr, newAddr)) {
            log.info("update master ha address, OLD: " + currentAddr + " NEW: " + newAddr);
        }
    }

    public void updateMasterAddress(final String newAddr) {
        String currentAddr = this.masterAddress.get();
        if (masterAddress.compareAndSet(currentAddr, newAddr)) {
            log.info("update master address, OLD: " + currentAddr + " NEW: " + newAddr);
        }
    }

    public String getHaMasterAddress() {
        return this.masterHaAddress.get();
    }

    public String getMasterAddress() {
        return this.masterAddress.get();
    }

    private boolean isTimeToReportOffset() {
        long interval = defaultMessageStore.now() - this.lastWriteTimestamp;
        return interval > defaultMessageStore.getMessageStoreConfig().getHaSendHeartbeatInterval();
    }

    private boolean reportSlaveMaxOffset(final long maxOffset) {
        this.reportOffset.position(0);
        this.reportOffset.limit(REPORT_HEADER_SIZE);
        this.reportOffset.putLong(maxOffset);
        this.reportOffset.position(0);
        this.reportOffset.limit(REPORT_HEADER_SIZE);

        for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
            try {
                this.socketChannel.write(this.reportOffset);
            } catch (IOException e) {
                log.error(this.getServiceName()
                        + "reportSlaveMaxOffset this.socketChannel.write exception", e);
                return false;
            }
        }
        lastWriteTimestamp = this.defaultMessageStore.getSystemClock().now();
        return !this.reportOffset.hasRemaining();
    }

    private void reallocateByteBuffer() {
        int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
        if (remain > 0) {
            this.byteBufferRead.position(this.dispatchPosition);

            this.byteBufferBackup.position(0);
            this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
            this.byteBufferBackup.put(this.byteBufferRead);
        }

        this.swapByteBuffer();

        this.byteBufferRead.position(remain);
        this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        this.dispatchPosition = 0;
    }

    private void swapByteBuffer() {
        ByteBuffer tmp = this.byteBufferRead;
        this.byteBufferRead = this.byteBufferBackup;
        this.byteBufferBackup = tmp;
    }

    private boolean processReadEvent() {
        int readSizeZeroTimes = 0;
        //如果读取缓存还有没读取完，则一直读取
        while (this.byteBufferRead.hasRemaining()) {
            try {
                //从master读取消息
                int readSize = this.socketChannel.read(this.byteBufferRead);
                if (readSize > 0) {
                    flowMonitor.addByteCountTransferred(readSize);
                    readSizeZeroTimes = 0;
                    //分发请求
                    boolean result = this.dispatchReadRequest();
                    if (!result) {
                        log.error("HAClient, dispatchReadRequest error");
                        return false;
                    }
                    lastReadTimestamp = System.currentTimeMillis();
                } else if (readSize == 0) {
                    if (++readSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    log.info("HAClient, processReadEvent read socket < 0");
                    return false;
                }
            } catch (IOException e) {
                log.info("HAClient, processReadEvent read socket exception", e);
                return false;
            }
        }

        return true;
    }

    private boolean dispatchReadRequest() {
        //获取请求长度
        int readSocketPos = this.byteBufferRead.position();

        while (true) {
            //获取分发的偏移差
            int diff = this.byteBufferRead.position() - this.dispatchPosition;
            //如果偏移差大于头大小，说明存在请求体
            if (diff >= DefaultHAConnection.TRANSFER_HEADER_SIZE) {
                //获取主master的最大偏移量
                long masterPhyOffset = this.byteBufferRead.getLong(this.dispatchPosition);
                //获取消息体
                int bodySize = this.byteBufferRead.getInt(this.dispatchPosition + 8);

                //获取salve的最大偏移量
                long slavePhyOffset = this.defaultMessageStore.getMaxPhyOffset();

                if (slavePhyOffset != 0) {
                    if (slavePhyOffset != masterPhyOffset) {
                        log.error("master pushed offset not equal the max phy offset in slave, SLAVE: "
                                + slavePhyOffset + " MASTER: " + masterPhyOffset);
                        return false;
                    }
                }

                //如果偏移差大于 消息头和 消息体大小。则读取消息体
                if (diff >= (DefaultHAConnection.TRANSFER_HEADER_SIZE + bodySize)) {
                    byte[] bodyData = byteBufferRead.array();
                    int dataStart = this.dispatchPosition + DefaultHAConnection.TRANSFER_HEADER_SIZE;

                    //吧消息同步到slave的 CommitLog
                    this.defaultMessageStore.appendToCommitLog(
                            masterPhyOffset, bodyData, dataStart, bodySize);

                    this.byteBufferRead.position(readSocketPos);
                    //记录分发的位置
                    this.dispatchPosition += DefaultHAConnection.TRANSFER_HEADER_SIZE + bodySize;

                    if (!reportSlaveMaxOffsetPlus()) {
                        return false;
                    }

                    continue;
                }
            }

            if (!this.byteBufferRead.hasRemaining()) {
                this.reallocateByteBuffer();
            }

            break;
        }

        return true;
    }

    private boolean reportSlaveMaxOffsetPlus() {
        boolean result = true;
        long currentPhyOffset = this.defaultMessageStore.getMaxPhyOffset();
        if (currentPhyOffset > this.currentReportedOffset) {
            this.currentReportedOffset = currentPhyOffset;
            result = this.reportSlaveMaxOffset(this.currentReportedOffset);
            if (!result) {
                this.closeMaster();
                log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
            }
        }

        return result;
    }

    public void changeCurrentState(HAConnectionState currentState) {
        log.info("change state to {}", currentState);
        this.currentState = currentState;
    }

    public boolean connectMaster() throws ClosedChannelException {
        if (null == socketChannel) {
            String addr = this.masterHaAddress.get();
            if (addr != null) {
                SocketAddress socketAddress = NetworkUtil.string2SocketAddress(addr);
                this.socketChannel = RemotingHelper.connect(socketAddress);
                if (this.socketChannel != null) {
                    this.socketChannel.register(this.selector, SelectionKey.OP_READ);
                    log.info("HAClient connect to master {}", addr);
                    this.changeCurrentState(HAConnectionState.TRANSFER);
                }
            }

            this.currentReportedOffset = this.defaultMessageStore.getMaxPhyOffset();

            this.lastReadTimestamp = System.currentTimeMillis();
        }

        return this.socketChannel != null;
    }

    public void closeMaster() {
        if (null != this.socketChannel) {
            try {

                SelectionKey sk = this.socketChannel.keyFor(this.selector);
                if (sk != null) {
                    sk.cancel();
                }

                this.socketChannel.close();

                this.socketChannel = null;

                log.info("HAClient close connection with master {}", this.masterHaAddress.get());
                this.changeCurrentState(HAConnectionState.READY);
            } catch (IOException e) {
                log.warn("closeMaster exception. ", e);
            }

            this.lastReadTimestamp = 0;
            this.dispatchPosition = 0;

            this.byteBufferBackup.position(0);
            this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);

            this.byteBufferRead.position(0);
            this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        }
    }

    /**
     * 主要的逻辑如下：
     *
     * 连接master，如果当前的broker角色是master，那么对应的masterAddress是空的，不会有后续逻辑。如果是slave，并且配置了master地址，则会进行连接进行后续逻辑处理
     * 检查是否需要向master汇报当前的同步进度，如果两次同步的时间小于5s，则不进行同步。每次同步之间间隔在5s以上，这个5s是心跳连接的间隔参数为haSendHeartbeatInterval
     * 向master 汇报当前 salve 的CommitLog的最大偏移量,并记录这次的同步时间
     * 从master拉取日志信息，主要就是进行消息的同步，同步出问题则关闭连接
     * 再次同步slave的偏移量，如果最新的偏移量大于已经汇报的情况下则从步骤1重头开始
     * ————————————————
     * 版权声明：本文为CSDN博主「szhlcy」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
     * 原文链接：https://blog.csdn.net/szhlcy/article/details/116055627
     */
    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        this.flowMonitor.start();

        while (!this.isStopped()) {
            try {
                switch (this.currentState) {
                    case SHUTDOWN:
                        this.flowMonitor.shutdown(true);
                        return;
                    case READY:
                        if (!this.connectMaster()) {
                            log.warn("HAClient connect to master {} failed", this.masterHaAddress.get());
                            this.waitForRunning(1000 * 5);
                        }
                        continue;
                    case TRANSFER:
                        if (!transferFromMaster()) {
                            closeMasterAndWait();
                            continue;
                        }
                        break;
                    default:
                        this.waitForRunning(1000 * 2);
                        continue;
                }

                long interval = this.defaultMessageStore.now() - this.lastReadTimestamp;
                if (interval > this.defaultMessageStore.getMessageStoreConfig().getHaHousekeepingInterval()) {
                    log.warn("AutoRecoverHAClient, housekeeping, found this connection[" + this.masterHaAddress
                            + "] expired, " + interval);
                    this.closeMaster();
                    log.warn("AutoRecoverHAClient, master not response some time, so close connection");
                }
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
                this.closeMasterAndWait();
            }
        }

        this.flowMonitor.shutdown(true);
        log.info(this.getServiceName() + " service end");
    }

    private boolean transferFromMaster() throws IOException {
        boolean result;
        if (this.isTimeToReportOffset()) {
            log.info("Slave report current offset {}", this.currentReportedOffset);
            result = this.reportSlaveMaxOffset(this.currentReportedOffset);
            if (!result) {
                return false;
            }
        }

        this.selector.select(1000);

        result = this.processReadEvent();
        if (!result) {
            return false;
        }

        return reportSlaveMaxOffsetPlus();
    }

    public void closeMasterAndWait() {
        this.closeMaster();
        this.waitForRunning(1000 * 5);
    }

    public long getLastWriteTimestamp() {
        return this.lastWriteTimestamp;
    }

    public long getLastReadTimestamp() {
        return lastReadTimestamp;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public long getTransferredByteInSecond() {
        return flowMonitor.getTransferredByteInSecond();
    }

    @Override
    public void shutdown() {
        this.changeCurrentState(HAConnectionState.SHUTDOWN);
        this.flowMonitor.shutdown();
        super.shutdown();

        closeMaster();
        try {
            this.selector.close();
        } catch (IOException e) {
            log.warn("Close the selector of AutoRecoverHAClient error, ", e);
        }
    }

    @Override
    public String getServiceName() {
        if (this.defaultMessageStore != null && this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return this.defaultMessageStore.getBrokerIdentity().getIdentifier() + DefaultHAClient.class.getSimpleName();
        }
        return DefaultHAClient.class.getSimpleName();
    }
}
