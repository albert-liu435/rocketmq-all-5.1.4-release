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

import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.message.MessageVersion;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.common.utils.QueueTypeUtils;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.MessageExtEncoder.PutMessageThreadLocal;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * https://www.jianshu.com/p/ad3ad3a648ee
 * 消息存储，所有消息主题的消息都存储在CommitLog文件中。
 * <p>
 * <p>
 * commitlog文件由CommitLog类提供服务（大文件）
 * Store all metadata downtime for recovery, data protection reliability
 * <p>
 * 消息主体以及元数据的存储主体，存储Producer端写入的消息主体内容,消息内容不是定长的。单个文件大小默认1G ，文件名长度为20位，
 * 左边补零，剩余为起始偏移量，比如00000000000000000000代表了第一个文件，起始偏移量为0，文件大小为1G=1073741824；
 * 当第一个文件写满了，第二个文件为00000000001073741824，起始偏移量为1073741824，以此类推。消息主要是顺序写入日志文件，
 * 当文件满了，写入下一个文件
 * <p>
 * RocketMQ的存储与读写是基于JDK NIO的内存映射机制
 * （MappedByteBuffer）的，消息存储时首先将消息追加到内存中，再
 * 根据配置的刷盘策略在不同时间刷盘。如果是同步刷盘，消息追加到
 * 内存后，将同步调用MappedByteBuffer的force()方法；如果是异步刷
 * 盘，在消息追加到内存后会立刻返回给消息发送端。RocketMQ使用一
 * 个单独的线程按照某一个设定的频率执行刷盘操作。通过在broker配
 * 置文件中配置flushDiskType来设定刷盘方式，可选值为
 * ASYNC_FLUSH（异步刷盘）、SYNC_FLUSH（同步刷盘），默认为异步刷
 * 盘。本节以CommitLog文件刷盘机制为例来剖析RocketMQ的刷盘机制，
 * ConsumeQueue文件、Index文件刷盘的实现原理与CommitLog刷盘机制
 * 类似。RocketMQ处理刷盘的实现方法为
 * Commitlog#handleDiskFlush()，刷盘流程作为消息发送、消息存储的
 * 子流程，我们先重点了解消息存储流程的相关知识。值得注意的是，
 * Index文件的刷盘并不是采取定时刷盘机制，而是每更新一次Index文
 * 件就会将上一次的改动写入磁盘。
 */
public class CommitLog implements Swappable {


//    顺序编号 |字段简称 |字段大小（字节）| 字段含义
//      ---|---|---|---|---
//1 | msgSize |4| 代表这个消息的大小
//2 |MAGICCODE| 4 |MAGICCODE = daa320a7
//3 |BODY CRC |4 |消息体BODY CRC 当broker重启recover时会校验
//4 |queueId |4 |消息队列id
//5 |flag| 4 |
//6 |QUEUEOFFSET| 8 |这个值是个自增值不是真正的consume queue的偏移量，可以代表这个consumeQueue队列或者tranStateTable队列中消息的个数，若是非事务消息或者commit事务消息，可以通过这个值查找到consumeQueue中数据，QUEUEOFFSET * 20才是偏移地址；若是PREPARED或者Rollback事务，则可以通过该值从tranStateTable中查找数据
//7| PHYSICALOFFSET |8 |代表消息在commitLog中的物理起始地址偏移量
//8 |SYSFLAG| 4 |指明消息是事物事物状态等消息特征，二进制为四个字节从右往左数：当4个字节均为0（值为0）时表示非事务消息；当第1个字节为1（值为1）时表示表示消息是压缩的（Compressed）；当第2个字节为1（值为2）表示多消息（MultiTags）；当第3个字节为1（值为4）时表示prepared消息；当第4个字节为1（值为8）时表示commit消息；当第3/4个字节均为1时（值为12）时表示rollback消息；当第3/4个字节均为0时表示非事务消息；
//9 |BORNTIMESTAMP| 8 |消息产生端(producer)的时间戳
//10| BORNHOST |8| 消息产生端(producer)地址(address:port)
//11| STORETIMESTAMP |8 |消息在broker存储时间
//12| STOREHOSTADDRESS |8 |消息存储到broker的地址(address:port)
//13| RECONSUMETIMES| 8 |消息被某个订阅组重新消费了几次（订阅组之间独立计数）,因为重试消息发送到了topic名字为%retry%groupName的队列queueId=0的队列中去了，成功消费一次记录为0；
//14| PreparedTransaction Offset| 8 |表示是prepared状态的事物消息
//15| messagebodyLength |4| 消息体大小值
//16| messagebody| bodyLength| 消息体内容
//17| topicLength| 1 |topic名称内容大小
//18| topic |topicLength |topic的内容值
//19| propertiesLength| 2 |属性值大小
//20| properties |propertiesLength |propertiesLength大小的属性数据
//
// 可以看到CommitLog文件的一个消息体的长度是不确定的，但是有字段messagebodyLength来表示的是消息体大小和propertiesLength表示属性值的大小。所以可以计算出这个消息数据的大小。


    // Message's MAGIC CODE daa320a7
    //用来验证消息的合法性，类似于java的魔数的作用
    public final static int MESSAGE_MAGIC_CODE = -626843481;
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    //消息不够存储的时候用这个来表示
    // End of file empty MAGIC CODE cbd43194
    public final static int BLANK_MAGIC_CODE = -875286124;
    //映射文件集合
    protected final MappedFileQueue mappedFileQueue;
    //默认消息存储类，CommitLog的所有操作都是通过DefaultMessageStore来进行的
    //用于操作CommitLog类的对象
    protected final DefaultMessageStore defaultMessageStore;

    //刷盘管理器
    private final FlushManager flushManager;

    private final ColdDataCheckService coldDataCheckService;
    //追加消息回调函数
    private final AppendMessageCallback appendMessageCallback;

    private final ThreadLocal<PutMessageThreadLocal> putMessageThreadLocal;

    protected volatile long confirmOffset = -1L;

    private volatile long beginTimeInLock = 0;
    //写入消息锁
    protected final PutMessageLock putMessageLock;

    protected final TopicQueueLock topicQueueLock;

    private volatile Set<String> fullStorePaths = Collections.emptySet();

    private final FlushDiskWatcher flushDiskWatcher;

    //commitlog大小   为1G
    protected int commitLogSize;

    public CommitLog(final DefaultMessageStore messageStore) {

        // CommitLog 存储路径，默认在 ${storePathRootDir}/commitlog
        String storePath = messageStore.getMessageStoreConfig().getStorePathCommitLog();

        //创建MappedFileQueue对象，传入的路径是配置的CommitLog的文件路径，和默认的文件大小1G，同时传入提前创建MappedFile对象的AllocateMappedFileService
        if (storePath.contains(MixAll.MULTI_PATH_SPLITTER)) {
            this.mappedFileQueue = new MultiPathMappedFileQueue(messageStore.getMessageStoreConfig(),
                    messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
                    messageStore.getAllocateMappedFileService(), this::getFullStorePaths);
        } else {
            // 将磁盘中的 CommitLog 做 MappedFile 内存映射
            this.mappedFileQueue = new MappedFileQueue(storePath,
                    // commitlog 文件大小默认为 1GB
                    messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog(),
                    // AllocateMappedFileService
                    messageStore.getAllocateMappedFileService());
        }

        this.defaultMessageStore = messageStore;

        this.flushManager = new DefaultFlushManager();
        this.coldDataCheckService = new ColdDataCheckService();
        //拼接消息的回调对象
        this.appendMessageCallback = new DefaultAppendMessageCallback();
        //定于对应的消息编码器，会设定消息的最大大小，默认是512k
        putMessageThreadLocal = new ThreadLocal<PutMessageThreadLocal>() {
            @Override
            protected PutMessageThreadLocal initialValue() {
                return new PutMessageThreadLocal(defaultMessageStore.getMessageStoreConfig().getMaxMessageSize());
            }
        };
        //存储消息的时候用自旋锁还是互斥锁(用的是JDK的ReentrantLock)，默认的是自旋锁（用的是JDK的原子类的AtomicBoolean）
        this.putMessageLock = messageStore.getMessageStoreConfig().isUseReentrantLockWhenPutMessage() ? new PutMessageReentrantLock() : new PutMessageSpinLock();

        this.flushDiskWatcher = new FlushDiskWatcher();

        this.topicQueueLock = new TopicQueueLock(messageStore.getMessageStoreConfig().getTopicQueueLockNum());

        this.commitLogSize = messageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
    }

    public void setFullStorePaths(Set<String> fullStorePaths) {
        this.fullStorePaths = fullStorePaths;
    }

    public Set<String> getFullStorePaths() {
        return fullStorePaths;
    }

    public long getTotalSize() {
        return this.mappedFileQueue.getTotalFileSize();
    }

    public ThreadLocal<PutMessageThreadLocal> getPutMessageThreadLocal() {
        return putMessageThreadLocal;
    }

    /**
     * 进行文件加载映射
     *
     * @return
     */
    public boolean load() {
        //加载映射文件集合
        boolean result = this.mappedFileQueue.load();

        if (result && !defaultMessageStore.getMessageStoreConfig().isDataReadAheadEnable()) {
            scanFileAndSetReadMode(LibC.MADV_RANDOM);
        }
        this.mappedFileQueue.checkSelf();
        log.info("load commit log " + (result ? "OK" : "Failed"));
        return result;
    }

    public void start() {
        this.flushManager.start();
        log.info("start commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        //设置为守护线程，守护线程是优先级低，存活与否不影响JVM的退出的线程
        flushDiskWatcher.setDaemon(true);
        flushDiskWatcher.start();
        if (this.coldDataCheckService != null) {
            this.coldDataCheckService.start();
        }
    }

    public void shutdown() {
        this.flushManager.shutdown();
        log.info("shutdown commitLog successfully. storeRoot: {}", this.defaultMessageStore.getMessageStoreConfig().getStorePathRootDir());
        flushDiskWatcher.shutdown(true);
        if (this.coldDataCheckService != null) {
            this.coldDataCheckService.shutdown();
        }
    }

    public long flush() {
        this.mappedFileQueue.commit(0);
        this.mappedFileQueue.flush(0);
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getFlushedWhere() {
        return this.mappedFileQueue.getFlushedWhere();
    }

    public long getMaxOffset() {
        return this.mappedFileQueue.getMaxOffset();
    }

    public long remainHowManyDataToCommit() {
        return this.mappedFileQueue.remainHowManyDataToCommit();
    }

    public long remainHowManyDataToFlush() {
        return this.mappedFileQueue.remainHowManyDataToFlush();
    }

    public int deleteExpiredFile(
            final long expiredTime,
            final int deleteFilesInterval,
            final long intervalForcibly,
            final boolean cleanImmediately
    ) {
        return deleteExpiredFile(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately, 0);
    }

    public int deleteExpiredFile(
            final long expiredTime,
            final int deleteFilesInterval,
            final long intervalForcibly,
            final boolean cleanImmediately,
            final int deleteFileBatchMax
    ) {
        return this.mappedFileQueue.deleteExpiredFileByTime(expiredTime, deleteFilesInterval, intervalForcibly, cleanImmediately, deleteFileBatchMax);
    }

    /**
     * 获取消息
     * <p>
     *  这个方法会返回传入的偏移量所在的消息的buffer
     * Read CommitLog data, use data replication
     */
    public SelectMappedBufferResult getData(final long offset) {
        return this.getData(offset, offset == 0);
    }

    /**
     * 接着看 MappedFile 里是如何根据偏移量读取数据的，它首先要判断读到什么位置（readPosition），这个值在 writeBuffer 存在的时候就是 committedPosition 的位置，这个位置之前的数据是已经commit到MappedByteBuffer了的；
     * 否则就是 wrotePosition 的位置，就是 MappedByteBuffer 当前写入的位置。然后根据 readPosition 和传入的要读的开始位置（pos），就能计算出要读取的数据大小（size）。
     *
     * @param offset
     * @param returnFirstOnNotFound
     * @return
     */
    public SelectMappedBufferResult getData(final long offset, final boolean returnFirstOnNotFound) {
        //获取配置的CommitLog 的文件大小
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        //按offset查询映射文件，如果在偏移量为0的时候，会返回新创建的CommitLog文件映射对象，因为这是第一次插入
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, returnFirstOnNotFound);
        if (mappedFile != null) {
            //位置=偏移量%文件大小
            int pos = (int) (offset % mappedFileSize);
            //然后从 MappedByteBuffer 创建当前数据的视图 byteBuffer，将当前position定位到要读的位置（pos），然后再得到一个新的视图 byteBufferNew，再限制它要读的数据大小（limit），
            // 这样byteBufferNew就是从 pos 到 readPosition 之间的数据了。
            SelectMappedBufferResult result = mappedFile.selectMappedBuffer(pos);
            return result;
        }

        return null;
    }

    public boolean getData(final long offset, final int size, final ByteBuffer byteBuffer) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            return mappedFile.getData(pos, size, byteBuffer);
        }
        return false;
    }

    public List<SelectMappedBufferResult> getBulkData(final long offset, final int size) {
        List<SelectMappedBufferResult> bufferResultList = new ArrayList<>();

        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        int remainSize = size;
        long startOffset = offset;
        long maxOffset = this.getMaxOffset();
        if (offset + size > maxOffset) {
            remainSize = (int) (maxOffset - offset);
            log.warn("get bulk data size out of range, correct to max offset. offset: {}, size: {}, max: {}", offset, remainSize, maxOffset);
        }

        while (remainSize > 0) {
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(startOffset, startOffset == 0);
            if (mappedFile != null) {
                int pos = (int) (startOffset % mappedFileSize);
                int readableSize = mappedFile.getReadPosition() - pos;
                int readSize = Math.min(remainSize, readableSize);

                SelectMappedBufferResult bufferResult = mappedFile.selectMappedBuffer(pos, readSize);
                if (bufferResult == null) {
                    break;
                }
                bufferResultList.add(bufferResult);
                remainSize -= readSize;
                startOffset += readSize;
            }
        }

        return bufferResultList;
    }

    public SelectMappedFileResult getFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int size = (int) (mappedFile.getReadPosition() - offset % mappedFileSize);
            if (size > 0) {
                return new SelectMappedFileResult(size, mappedFile);
            }
        }
        return null;
    }

    //Create new mappedFile if not exits.
    public boolean getLastMappedFile(final long startOffset) {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
        if (null == lastMappedFile) {
            log.error("getLastMappedFile error. offset:{}", startOffset);
            return false;
        }

        return true;
    }

    /**
     * recoverNormally方法在RocketMQ正常关闭然后启动的时候会调用，这个方法就是把加载的映射文件列表进行遍历，对文件进行校验，和文件中的消息的魔数进行校验，来判断哪些数据是正常的，并计算出正常的数据的最大偏移量。
     * 然后，根据偏移量设置对应的提交和刷新的位置以及不正常数据的删除。
     * ————————————————
     * 版权声明：本文为CSDN博主「szhlcy」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
     * 原文链接：https://blog.csdn.net/szhlcy/article/details/114952442
     * <p>
     * When the normal exit, data recovery, all memory data have been flush
     */
    public void recoverNormally(long maxPhyOffsetOfConsumeQueue) {
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        boolean checkDupInfo = this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Began to recover from the last third file
            int index = mappedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }

            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long lastValidMsgPhyOffset = this.getConfirmOffset();
            // normal recover doesn't require dispatching
            boolean doDispatch = false;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover, checkDupInfo);
                int size = dispatchRequest.getMsgSize();
                // Normal data
                if (dispatchRequest.isSuccess() && size > 0) {
                    lastValidMsgPhyOffset = processOffset + mappedFileOffset;
                    mappedFileOffset += size;
                    this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                }
                // Come the end of the file, switch to the next file Since the
                // return 0 representatives met last hole,
                // this can not be included in truncate offset
                else if (dispatchRequest.isSuccess() && size == 0) {
                    this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, true);
                    index++;
                    if (index >= mappedFiles.size()) {
                        // Current branch can not happen
                        log.info("recover last 3 physics file over, last mapped file " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next physics file, " + mappedFile.getFileName());
                    }
                }
                // Intermediate file read error
                else if (!dispatchRequest.isSuccess()) {
                    if (size > 0) {
                        log.warn("found a half message at {}, it will be truncated.", processOffset + mappedFileOffset);
                    }
                    log.info("recover physics file end, " + mappedFile.getFileName());
                    break;
                }
            }

            processOffset += mappedFileOffset;

            if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
                if (this.defaultMessageStore.getConfirmOffset() < this.defaultMessageStore.getMinPhyOffset()) {
                    log.error("confirmOffset {} is less than minPhyOffset {}, correct confirmOffset to minPhyOffset", this.defaultMessageStore.getConfirmOffset(), this.defaultMessageStore.getMinPhyOffset());
                    this.defaultMessageStore.setConfirmOffset(this.defaultMessageStore.getMinPhyOffset());
                } else if (this.defaultMessageStore.getConfirmOffset() > processOffset) {
                    log.error("confirmOffset {} is larger than processOffset {}, correct confirmOffset to processOffset", this.defaultMessageStore.getConfirmOffset(), processOffset);
                    this.defaultMessageStore.setConfirmOffset(processOffset);
                }
            } else {
                this.setConfirmOffset(lastValidMsgPhyOffset);
            }

            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
        } else {
            // Commitlog case files are deleted
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
                                                     final boolean checkDupInfo) {
        return this.checkMessageAndReturnSize(byteBuffer, checkCRC, checkDupInfo, true);
    }

    private void doNothingForDeadCode(final Object obj) {
        if (obj != null) {
            log.debug(String.valueOf(obj.hashCode()));
        }
    }

    /**
     * check the message and returns the message size
     *
     * @return 0 Come the end of the file // >0 Normal messages // -1 Message checksum failure
     */
    public DispatchRequest checkMessageAndReturnSize(java.nio.ByteBuffer byteBuffer, final boolean checkCRC,
                                                     final boolean checkDupInfo, final boolean readBody) {
        try {
            // 1 TOTAL SIZE
            int totalSize = byteBuffer.getInt();

            // 2 MAGIC CODE
            int magicCode = byteBuffer.getInt();
            switch (magicCode) {
                case MessageDecoder.MESSAGE_MAGIC_CODE:
                case MessageDecoder.MESSAGE_MAGIC_CODE_V2:
                    break;
                case BLANK_MAGIC_CODE:
                    return new DispatchRequest(0, true /* success */);
                default:
                    log.warn("found a illegal magic code 0x" + Integer.toHexString(magicCode));
                    return new DispatchRequest(-1, false /* success */);
            }

            MessageVersion messageVersion = MessageVersion.valueOfMagicCode(magicCode);

            byte[] bytesContent = new byte[totalSize];

            int bodyCRC = byteBuffer.getInt();

            int queueId = byteBuffer.getInt();

            int flag = byteBuffer.getInt();

            long queueOffset = byteBuffer.getLong();

            long physicOffset = byteBuffer.getLong();

            int sysFlag = byteBuffer.getInt();

            long bornTimeStamp = byteBuffer.getLong();

            ByteBuffer byteBuffer1;
            if ((sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0) {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer1 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }

            long storeTimestamp = byteBuffer.getLong();

            ByteBuffer byteBuffer2;
            if ((sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0) {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 4 + 4);
            } else {
                byteBuffer2 = byteBuffer.get(bytesContent, 0, 16 + 4);
            }

            int reconsumeTimes = byteBuffer.getInt();

            long preparedTransactionOffset = byteBuffer.getLong();

            int bodyLen = byteBuffer.getInt();
            if (bodyLen > 0) {
                if (readBody) {
                    byteBuffer.get(bytesContent, 0, bodyLen);

                    if (checkCRC) {
                        int crc = UtilAll.crc32(bytesContent, 0, bodyLen);
                        if (crc != bodyCRC) {
                            log.warn("CRC check failed. bodyCRC={}, currentCRC={}", crc, bodyCRC);
                            return new DispatchRequest(-1, false/* success */);
                        }
                    }
                } else {
                    byteBuffer.position(byteBuffer.position() + bodyLen);
                }
            }

            int topicLen = messageVersion.getTopicLength(byteBuffer);
            byteBuffer.get(bytesContent, 0, topicLen);
            String topic = new String(bytesContent, 0, topicLen, MessageDecoder.CHARSET_UTF8);

            long tagsCode = 0;
            String keys = "";
            String uniqKey = null;

            short propertiesLength = byteBuffer.getShort();
            Map<String, String> propertiesMap = null;
            if (propertiesLength > 0) {
                byteBuffer.get(bytesContent, 0, propertiesLength);
                String properties = new String(bytesContent, 0, propertiesLength, MessageDecoder.CHARSET_UTF8);
                propertiesMap = MessageDecoder.string2messageProperties(properties);

                keys = propertiesMap.get(MessageConst.PROPERTY_KEYS);

                uniqKey = propertiesMap.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);

                if (checkDupInfo) {
                    String dupInfo = propertiesMap.get(MessageConst.DUP_INFO);
                    if (null == dupInfo || dupInfo.split("_").length != 2) {
                        log.warn("DupInfo in properties check failed. dupInfo={}", dupInfo);
                        return new DispatchRequest(-1, false);
                    }
                }

                String tags = propertiesMap.get(MessageConst.PROPERTY_TAGS);
                if (tags != null && tags.length() > 0) {
                    tagsCode = MessageExtBrokerInner.tagsString2tagsCode(MessageExt.parseTopicFilterType(sysFlag), tags);
                }

                // Timing message processing
                {
                    String t = propertiesMap.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL);
                    if (TopicValidator.RMQ_SYS_SCHEDULE_TOPIC.equals(topic) && t != null) {
                        int delayLevel = Integer.parseInt(t);

                        if (delayLevel > this.defaultMessageStore.getMaxDelayLevel()) {
                            delayLevel = this.defaultMessageStore.getMaxDelayLevel();
                        }

                        if (delayLevel > 0) {
                            tagsCode = this.defaultMessageStore.computeDeliverTimestamp(delayLevel,
                                    storeTimestamp);
                        }
                    }
                }
            }

            int readLength = MessageExtEncoder.calMsgLength(messageVersion, sysFlag, bodyLen, topicLen, propertiesLength);
            if (totalSize != readLength) {
                doNothingForDeadCode(reconsumeTimes);
                doNothingForDeadCode(flag);
                doNothingForDeadCode(bornTimeStamp);
                doNothingForDeadCode(byteBuffer1);
                doNothingForDeadCode(byteBuffer2);
                log.error(
                        "[BUG]read total count not equals msg total size. totalSize={}, readTotalCount={}, bodyLen={}, topicLen={}, propertiesLength={}",
                        totalSize, readLength, bodyLen, topicLen, propertiesLength);
                return new DispatchRequest(totalSize, false/* success */);
            }

            DispatchRequest dispatchRequest = new DispatchRequest(
                    topic,
                    queueId,
                    physicOffset,
                    totalSize,
                    tagsCode,
                    storeTimestamp,
                    queueOffset,
                    keys,
                    uniqKey,
                    sysFlag,
                    preparedTransactionOffset,
                    propertiesMap
            );

            setBatchSizeIfNeeded(propertiesMap, dispatchRequest);

            return dispatchRequest;
        } catch (Exception e) {
        }

        return new DispatchRequest(-1, false /* success */);
    }

    private void setBatchSizeIfNeeded(Map<String, String> propertiesMap, DispatchRequest dispatchRequest) {
        if (null != propertiesMap && propertiesMap.containsKey(MessageConst.PROPERTY_INNER_NUM) && propertiesMap.containsKey(MessageConst.PROPERTY_INNER_BASE)) {
            dispatchRequest.setMsgBaseOffset(Long.parseLong(propertiesMap.get(MessageConst.PROPERTY_INNER_BASE)));
            dispatchRequest.setBatchSize(Short.parseShort(propertiesMap.get(MessageConst.PROPERTY_INNER_NUM)));
        }
    }

    // Fetch and compute the newest confirmOffset.
    // Even if it is just inited.
    public long getConfirmOffset() {
        if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE && !this.defaultMessageStore.getRunningFlags().isFenced()) {
                if (((AutoSwitchHAService) this.defaultMessageStore.getHaService()).getLocalSyncStateSet().size() == 1) {
                    return this.defaultMessageStore.getMaxPhyOffset();
                }
                // First time it will compute the confirmOffset.
                if (this.confirmOffset <= 0) {
                    setConfirmOffset(((AutoSwitchHAService) this.defaultMessageStore.getHaService()).computeConfirmOffset());
                    log.info("Init the confirmOffset to {}.", this.confirmOffset);
                }
            }
            return this.confirmOffset;
        } else if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return this.confirmOffset;
        } else {
            return getMaxOffset();
        }
    }

    // Fetch the original confirmOffset's value.
    // Without checking and re-computing.
    public long getConfirmOffsetDirectly() {
        if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE && !this.defaultMessageStore.getRunningFlags().isFenced()) {
                if (((AutoSwitchHAService) this.defaultMessageStore.getHaService()).getLocalSyncStateSet().size() == 1) {
                    return this.defaultMessageStore.getMaxPhyOffset();
                }
            }
            return this.confirmOffset;
        } else if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return this.confirmOffset;
        } else {
            return getMaxOffset();
        }
    }

    public void setConfirmOffset(long phyOffset) {
        this.confirmOffset = phyOffset;
        this.defaultMessageStore.getStoreCheckpoint().setConfirmPhyOffset(confirmOffset);
    }

    public long getLastFileFromOffset() {
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        if (lastMappedFile != null) {
            if (lastMappedFile.isAvailable()) {
                return lastMappedFile.getFileFromOffset();
            }
        }

        return -1;
    }

    @Deprecated
    public void recoverAbnormally(long maxPhyOffsetOfConsumeQueue) {
        // recover by the minimum time stamp
        boolean checkCRCOnRecover = this.defaultMessageStore.getMessageStoreConfig().isCheckCRCOnRecover();
        boolean checkDupInfo = this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable();
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            // Looking beginning to recover from which file
            int index = mappedFiles.size() - 1;
            MappedFile mappedFile = null;
            for (; index >= 0; index--) {
                mappedFile = mappedFiles.get(index);
                if (this.isMappedFileMatchedRecover(mappedFile)) {
                    log.info("recover from this mapped file " + mappedFile.getFileName());
                    break;
                }
            }

            if (index < 0) {
                index = 0;
                mappedFile = mappedFiles.get(index);
            }

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long lastValidMsgPhyOffset = processOffset;
            long lastConfirmValidMsgPhyOffset = processOffset;
            // abnormal recover require dispatching
            boolean doDispatch = true;
            while (true) {
                DispatchRequest dispatchRequest = this.checkMessageAndReturnSize(byteBuffer, checkCRCOnRecover, checkDupInfo);
                int size = dispatchRequest.getMsgSize();

                if (dispatchRequest.isSuccess()) {
                    // Normal data
                    if (size > 0) {
                        lastValidMsgPhyOffset = processOffset + mappedFileOffset;
                        mappedFileOffset += size;

                        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable() || this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
                            if (dispatchRequest.getCommitLogOffset() + size <= this.defaultMessageStore.getCommitLog().getConfirmOffset()) {
                                this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                                lastConfirmValidMsgPhyOffset = dispatchRequest.getCommitLogOffset() + size;
                            }
                        } else {
                            this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, false);
                        }
                    }
                    // Come the end of the file, switch to the next file
                    // Since the return 0 representatives met last hole, this can
                    // not be included in truncate offset
                    else if (size == 0) {
                        this.getMessageStore().onCommitLogDispatch(dispatchRequest, doDispatch, mappedFile, true, true);
                        index++;
                        if (index >= mappedFiles.size()) {
                            // The current branch under normal circumstances should
                            // not happen
                            log.info("recover physics file over, last mapped file " + mappedFile.getFileName());
                            break;
                        } else {
                            mappedFile = mappedFiles.get(index);
                            byteBuffer = mappedFile.sliceByteBuffer();
                            processOffset = mappedFile.getFileFromOffset();
                            mappedFileOffset = 0;
                            log.info("recover next physics file, " + mappedFile.getFileName());
                        }
                    }
                } else {

                    if (size > 0) {
                        log.warn("found a half message at {}, it will be truncated.", processOffset + mappedFileOffset);
                    }

                    log.info("recover physics file end, " + mappedFile.getFileName() + " pos=" + byteBuffer.position());
                    break;
                }
            }

            processOffset += mappedFileOffset;
            if (this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
                if (this.defaultMessageStore.getConfirmOffset() < this.defaultMessageStore.getMinPhyOffset()) {
                    log.error("confirmOffset {} is less than minPhyOffset {}, correct confirmOffset to minPhyOffset", this.defaultMessageStore.getConfirmOffset(), this.defaultMessageStore.getMinPhyOffset());
                    this.defaultMessageStore.setConfirmOffset(this.defaultMessageStore.getMinPhyOffset());
                } else if (this.defaultMessageStore.getConfirmOffset() > lastConfirmValidMsgPhyOffset) {
                    log.error("confirmOffset {} is larger than lastConfirmValidMsgPhyOffset {}, correct confirmOffset to lastConfirmValidMsgPhyOffset", this.defaultMessageStore.getConfirmOffset(), lastConfirmValidMsgPhyOffset);
                    this.defaultMessageStore.setConfirmOffset(lastConfirmValidMsgPhyOffset);
                }
            } else {
                this.setConfirmOffset(lastValidMsgPhyOffset);
            }
            this.mappedFileQueue.setFlushedWhere(processOffset);
            this.mappedFileQueue.setCommittedWhere(processOffset);
            this.mappedFileQueue.truncateDirtyFiles(processOffset);

            // Clear ConsumeQueue redundant data
            if (maxPhyOffsetOfConsumeQueue >= processOffset) {
                log.warn("maxPhyOffsetOfConsumeQueue({}) >= processOffset({}), truncate dirty logic files", maxPhyOffsetOfConsumeQueue, processOffset);
                this.defaultMessageStore.truncateDirtyLogicFiles(processOffset);
            }
        }
        // Commitlog case files are deleted
        else {
            log.warn("The commitlog files are deleted, and delete the consume queue files");
            this.mappedFileQueue.setFlushedWhere(0);
            this.mappedFileQueue.setCommittedWhere(0);
            this.defaultMessageStore.destroyLogics();
        }
    }

    public void truncateDirtyFiles(long phyOffset) {
        if (phyOffset <= this.getFlushedWhere()) {
            this.mappedFileQueue.setFlushedWhere(phyOffset);
        }

        if (phyOffset <= this.mappedFileQueue.getCommittedWhere()) {
            this.mappedFileQueue.setCommittedWhere(phyOffset);
        }

        this.mappedFileQueue.truncateDirtyFiles(phyOffset);
        if (this.confirmOffset > phyOffset) {
            this.setConfirmOffset(phyOffset);
        }
    }

    protected void onCommitLogAppend(MessageExtBrokerInner msg, AppendMessageResult result, MappedFile commitLogFile) {
        this.getMessageStore().onCommitLogAppend(msg, result, commitLogFile);
    }

    private boolean isMappedFileMatchedRecover(final MappedFile mappedFile) {
        ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

        int magicCode = byteBuffer.getInt(MessageDecoder.MESSAGE_MAGIC_CODE_POSITION);
        if (magicCode != MessageDecoder.MESSAGE_MAGIC_CODE && magicCode != MessageDecoder.MESSAGE_MAGIC_CODE_V2) {
            return false;
        }

        int sysFlag = byteBuffer.getInt(MessageDecoder.SYSFLAG_POSITION);
        int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
        int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
        long storeTimestamp = byteBuffer.getLong(msgStoreTimePos);
        if (0 == storeTimestamp) {
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()
                && this.defaultMessageStore.getMessageStoreConfig().isMessageIndexSafe()) {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestampIndex()) {
                log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        } else {
            if (storeTimestamp <= this.defaultMessageStore.getStoreCheckpoint().getMinTimestamp()) {
                log.info("find check timestamp, {} {}",
                        storeTimestamp,
                        UtilAll.timeMillisToHumanString(storeTimestamp));
                return true;
            }
        }

        return false;
    }

    public boolean resetOffset(long offset) {
        return this.mappedFileQueue.resetOffset(offset);
    }

    public long getBeginTimeInLock() {
        return beginTimeInLock;
    }

    public String generateKey(StringBuilder keyBuilder, MessageExt messageExt) {
        keyBuilder.setLength(0);
        keyBuilder.append(messageExt.getTopic());
        keyBuilder.append('-');
        keyBuilder.append(messageExt.getQueueId());
        return keyBuilder.toString();
    }

    public void setMappedFileQueueOffset(final long phyOffset) {
        this.mappedFileQueue.setFlushedWhere(phyOffset);
        this.mappedFileQueue.setCommittedWhere(phyOffset);
    }

    public void updateMaxMessageSize(PutMessageThreadLocal putMessageThreadLocal) {
        // dynamically adjust maxMessageSize, but not support DLedger mode temporarily
        int newMaxMessageSize = this.defaultMessageStore.getMessageStoreConfig().getMaxMessageSize();
        if (newMaxMessageSize >= 10 &&
                putMessageThreadLocal.getEncoder().getMaxMessageBodySize() != newMaxMessageSize) {
            putMessageThreadLocal.getEncoder().updateEncoderBufferCapacity(newMaxMessageSize);
        }
    }

    /**
     * 因此对于Commitlog.asyncPutMessage来说，主要的工作就是2步：
     * 1.获取或者创建一个MappedFile
     * 2.调用appendMessage进行存储
     * <p>
     * <p>
     * 添加消息putMessage和putMessages
     *  putMessage和putMessages都是添加消息到CommitLog的方法，只不过一个是添加单个消息，一个是添加多个消息的。这里只讲解添加单个消息的，添加多个消息的大家可以自行查看源码。逻辑步骤如下：
     * <p>
     * 设置消息对应的存储时间并对消息体编码
     * 获取消息topic和queueId为后面使用
     * 获取消息的事务类型
     * 如果不是事务消息，或者是事务消息的提交阶段，则还原消息原来的topic和queueId
     * 获取存储锁
     * 进行消息的存储，如果期间文件满了则再次存储，出错则抛错
     * 释放锁和映射文件，增加对应的记录信息
     * 进行刷盘
     * 进行高可用刷盘
     *
     * <p>
     * 消息写入的主流程如下：
     * <p>
     * <p>
     * 设置消息存储时间戳、设置消息体 crc32 校验和，避免消息篡改
     * <p>
     * <p>
     * 设置消息来源服务器以及存储服务器的 IPv6 地址标识
     * <p>
     * <p>
     * 获取线程副本 PutMessageThreadLocal，它的内部有一个 MessageExtEncoder 的编码器用于编码消息。
     * MessageExtEncoder 在消息编码时，就是将消息的一个个属性写入到一个 ByteBuffer 里。正常情况下 PutMessageResult 返回为 null，如果编码失败，比如消息默认不能超过4MB，超长就会认为消息非法然后返回一个 PutMessageResult，这个时候就会直接返回。
     * <p>
     * <p>
     * 如果消息编码成功，就会将编码得到的 ByteBuffer 设置到 MessageExtBrokerInner 里面，然后创建写入消息上下文对象 PutMessageContext。
     * <p>
     * <p>
     * 接下来开始准备写入消息，先用 putMessageLock 加写锁，保证同一时刻只有一个线程能写入消息。从这里可以看出， CommitLog 写入消息是串行的，但后面的 IO 机制能保证串行写入的高性能。
     * 加锁之后，通过 MappedFileQueue 获取最后一个 MappedFile。前面已经大概了解到，commitlog 目录对应 MappedFileQueue，目录下的文件就对应多个 MappedFile，写满一个切换下一个，所以每次都应该写入最后一个。
     * 如果获取最后一个 MappedFile 返回 null，那么说明是程序初次启动，还没有 commitlog 文件。这时就会去获取偏移量为 0 的 MappedFile，也就是会创建第一个 commitlog 文件。
     * 接着就是向这个 MappedFile 追加消息 appendMessage，可以看到参数还传入了 AppendMessageCallback，这是在 MappedFile 写入消息后，就会执行这个回调。
     * <p>
     * <p>
     * 消息追加后，如果消息写满了，将会创建一个写的 MappedFile，继续写入消息。消息写入完成后，最后在 finally 中释放 putMessageLock 锁。
     * <p>
     * <p>
     * 如果MappedFile写满了，且启用了预热机制的情况下，就会调用 unlockMappedFile 解锁文件。这个其实是创建 MappedFile 的时候，如果启用了预热机制，就会提前把磁盘数据加载到内存区域，并调用mlock系统调用锁定 MappedFile 对应的内存区域。这里就是在写满 MappedFile 后，调用munlock系统调用解锁之前锁定的内存区域。这个我们讲 MappedFile 的时候再详细看。
     * <p>
     * <p>
     * 写入消息完了之后就是增加统计信息，统计topic写入次数以及写入消息总量等。
     * <p>
     * <p>
     * 最后就是同步提交 flush 请求和 replica 请求，就是强制刷盘，并将数据同步到 slave 节点，最后返回追加消息结果。
     *
     * @param msg
     * @return
     */
    public CompletableFuture<PutMessageResult> asyncPutMessage(final MessageExtBrokerInner msg) {
        // 设置存储时间戳
        if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            msg.setStoreTimestamp(System.currentTimeMillis());
        }

        //CRC校验码
        // Set the message body CRC (consider the most appropriate setting on the client)
        // 设置消息体 crc32 校验和
        msg.setBodyCRC(UtilAll.crc32(msg.getBody()));
        // 写入消息返回结果
        // Back to Results
        AppendMessageResult result = null;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();
        //从消息中获取topic
        String topic = msg.getTopic();
        //设置版本
        msg.setVersion(MessageVersion.MESSAGE_VERSION_V1);

        boolean autoMessageVersionOnTopicLen =
                this.defaultMessageStore.getMessageStoreConfig().isAutoMessageVersionOnTopicLen();
        //设置版本为2
        if (autoMessageVersionOnTopicLen && topic.length() > Byte.MAX_VALUE) {
            msg.setVersion(MessageVersion.MESSAGE_VERSION_V2);
        }
        // 消息诞生机器 IPv6 地址标识
        InetSocketAddress bornSocketAddress = (InetSocketAddress) msg.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setBornHostV6Flag();
        }
        // 消息存储机器 IPv6 地址标识
        InetSocketAddress storeSocketAddress = (InetSocketAddress) msg.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            msg.setStoreHostAddressV6Flag();
        }

        // 获取线程副本中的消息编码器对消息编码
        PutMessageThreadLocal putMessageThreadLocal = this.putMessageThreadLocal.get();
        updateMaxMessageSize(putMessageThreadLocal);
        //主题队列key
        String topicQueueKey = generateKey(putMessageThreadLocal.getKeyBuilder(), msg);
        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;
        //获取最后一个mappedFile
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        long currOffset;
        if (mappedFile == null) {
            currOffset = 0;
        } else {
            //当前的偏移量
            currOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        //需要确认的副本数
        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        //是否需要HA
        boolean needHandleHA = needHandleHA(msg);

        if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset) < this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas()) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
            if (this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                // -1 means all ack in SyncStateSet
                needAckNums = MixAll.ALL_ACK_IN_SYNC_STATE_SET;
            }
        } else if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableSlaveActingMaster()) {
            int inSyncReplicas = Math.min(this.defaultMessageStore.getAliveReplicaNumInGroup(),
                    this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset));
            needAckNums = calcNeedAckNums(inSyncReplicas);
            if (needAckNums > inSyncReplicas) {
                // Tell the producer, don't have enough slaves to handle the send request
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
        }
        //上锁
        topicQueueLock.lock(topicQueueKey);
        try {

            boolean needAssignOffset = true;
            if (defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()
                    && defaultMessageStore.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE) {
                needAssignOffset = false;
            }
            if (needAssignOffset) {
                defaultMessageStore.assignOffset(msg);
            }
            //对消息进行编码
            PutMessageResult encodeResult = putMessageThreadLocal.getEncoder().encode(msg);
            //表示编码错误
            if (encodeResult != null) {
                return CompletableFuture.completedFuture(encodeResult);
            }
            // 设置消息编码后的 ByteBuffer
            msg.setEncodedBuff(putMessageThreadLocal.getEncoder().getEncoderBuffer());
            // 创建写入消息上下文
            PutMessageContext putMessageContext = new PutMessageContext(topicQueueKey);

            //可能会有多个线程并发请求，虽然支持集群，但是对于每个单独的broker都是本地存储，所以内存锁就足够了
            putMessageLock.lock(); //spin or ReentrantLock ,depending on store config
            try {

                long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
                this.beginTimeInLock = beginLockTimestamp;

                // Here settings are stored timestamp, in order to ensure an orderly
                // global
                if (!defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
                    msg.setStoreTimestamp(beginLockTimestamp);
                }

                //如果文件为空，或者已经存满，则创建一个新的commitlog文件
                if (null == mappedFile || mappedFile.isFull()) {
                    // 获取 MappedFile
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
                    if (isCloseReadAhead()) {
                        setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                    }
                }
                if (null == mappedFile) {
                    log.error("create mapped file1 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
                }

                // 向 MappedFile 追加消息
                //映射文件中添加消息，这里的appendMessageCallback是消息拼接对象，拼接过程不分析
                //调用底层的mappedFile进行出处，但是注意此时还没有刷盘
                result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                switch (result.getStatus()) {
                    case PUT_OK:

                        onCommitLogAppend(msg, result, mappedFile);
                        break;
                    //映射文件满了
                    case END_OF_FILE:
                        onCommitLogAppend(msg, result, mappedFile);
                        unlockMappedFile = mappedFile;
                        //创建一个文件来进行存储
                        // Create a new file, re-write the message
                        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                        if (null == mappedFile) {
                            // XXX: warn and notify me
                            log.error("create mapped file2 error, topic: " + msg.getTopic() + " clientAddr: " + msg.getBornHostString());
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                        }
                        if (isCloseReadAhead()) {
                            setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                        }
                        //重新添加消息=》
                        result = mappedFile.appendMessage(msg, this.appendMessageCallback, putMessageContext);
                        if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                            onCommitLogAppend(msg, result, mappedFile);
                        }
                        break;
                    // 消息过大
                    case MESSAGE_SIZE_EXCEEDED:
                        //消息属性过大
                    case PROPERTIES_SIZE_EXCEEDED:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                    case UNKNOWN_ERROR:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                    default:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                }

                elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
                beginTimeInLock = 0;
            } finally {
                //释放锁
                putMessageLock.unlock();
            }
            // Increase queue offset when messages are successfully written
            if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                this.defaultMessageStore.increaseOffset(msg, getMessageNum(msg));
            }
        } finally {
            //释放锁
            topicQueueLock.unlock(topicQueueKey);
        }
        //消息存储的多定时间过长
        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessage in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, msg.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            //解锁映射文件
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }
        // 消息写入结果
        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics 单次存储消息topic次数
        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(msg.getTopic()).add(result.getMsgNum());
        //单次存储消息topic大小
        storeStatsService.getSinglePutMessageTopicSizeTotal(topic).add(result.getWroteBytes());

        //磁盘刷新和主从刷新
        return handleDiskFlushAndHA(putMessageResult, msg, needAckNums, needHandleHA);
    }

    public CompletableFuture<PutMessageResult> asyncPutMessages(final MessageExtBatch messageExtBatch) {
        messageExtBatch.setStoreTimestamp(System.currentTimeMillis());
        AppendMessageResult result;

        StoreStatsService storeStatsService = this.defaultMessageStore.getStoreStatsService();

        final int tranType = MessageSysFlag.getTransactionValue(messageExtBatch.getSysFlag());

        if (tranType != MessageSysFlag.TRANSACTION_NOT_TYPE) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }
        if (messageExtBatch.getDelayTimeLevel() > 0) {
            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, null));
        }

        InetSocketAddress bornSocketAddress = (InetSocketAddress) messageExtBatch.getBornHost();
        if (bornSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setBornHostV6Flag();
        }

        InetSocketAddress storeSocketAddress = (InetSocketAddress) messageExtBatch.getStoreHost();
        if (storeSocketAddress.getAddress() instanceof Inet6Address) {
            messageExtBatch.setStoreHostAddressV6Flag();
        }

        long elapsedTimeInLock = 0;
        MappedFile unlockMappedFile = null;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();

        long currOffset;
        if (mappedFile == null) {
            currOffset = 0;
        } else {
            currOffset = mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }

        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        boolean needHandleHA = needHandleHA(messageExtBatch);

        if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableControllerMode()) {
            if (this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset) < this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas()) {
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
            if (this.defaultMessageStore.getMessageStoreConfig().isAllAckInSyncStateSet()) {
                // -1 means all ack in SyncStateSet
                needAckNums = MixAll.ALL_ACK_IN_SYNC_STATE_SET;
            }
        } else if (needHandleHA && this.defaultMessageStore.getBrokerConfig().isEnableSlaveActingMaster()) {
            int inSyncReplicas = Math.min(this.defaultMessageStore.getAliveReplicaNumInGroup(),
                    this.defaultMessageStore.getHaService().inSyncReplicasNums(currOffset));
            needAckNums = calcNeedAckNums(inSyncReplicas);
            if (needAckNums > inSyncReplicas) {
                // Tell the producer, don't have enough slaves to handle the send request
                return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.IN_SYNC_REPLICAS_NOT_ENOUGH, null));
            }
        }

        messageExtBatch.setVersion(MessageVersion.MESSAGE_VERSION_V1);
        boolean autoMessageVersionOnTopicLen =
                this.defaultMessageStore.getMessageStoreConfig().isAutoMessageVersionOnTopicLen();
        if (autoMessageVersionOnTopicLen && messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            messageExtBatch.setVersion(MessageVersion.MESSAGE_VERSION_V2);
        }

        //fine-grained lock instead of the coarse-grained
        PutMessageThreadLocal pmThreadLocal = this.putMessageThreadLocal.get();
        updateMaxMessageSize(pmThreadLocal);
        MessageExtEncoder batchEncoder = pmThreadLocal.getEncoder();

        String topicQueueKey = generateKey(pmThreadLocal.getKeyBuilder(), messageExtBatch);

        PutMessageContext putMessageContext = new PutMessageContext(topicQueueKey);
        messageExtBatch.setEncodedBuff(batchEncoder.encode(messageExtBatch, putMessageContext));

        topicQueueLock.lock(topicQueueKey);
        try {
            defaultMessageStore.assignOffset(messageExtBatch);

            putMessageLock.lock();
            try {
                long beginLockTimestamp = this.defaultMessageStore.getSystemClock().now();
                this.beginTimeInLock = beginLockTimestamp;

                // Here settings are stored timestamp, in order to ensure an orderly
                // global
                messageExtBatch.setStoreTimestamp(beginLockTimestamp);

                if (null == mappedFile || mappedFile.isFull()) {
                    mappedFile = this.mappedFileQueue.getLastMappedFile(0); // Mark: NewFile may be cause noise
                    if (isCloseReadAhead()) {
                        setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                    }
                }
                if (null == mappedFile) {
                    log.error("Create mapped file1 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                    beginTimeInLock = 0;
                    return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, null));
                }

                result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
                switch (result.getStatus()) {
                    case PUT_OK:
                        break;
                    case END_OF_FILE:
                        unlockMappedFile = mappedFile;
                        // Create a new file, re-write the message
                        mappedFile = this.mappedFileQueue.getLastMappedFile(0);
                        if (null == mappedFile) {
                            // XXX: warn and notify me
                            log.error("Create mapped file2 error, topic: {} clientAddr: {}", messageExtBatch.getTopic(), messageExtBatch.getBornHostString());
                            beginTimeInLock = 0;
                            return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.CREATE_MAPPED_FILE_FAILED, result));
                        }
                        if (isCloseReadAhead()) {
                            setFileReadMode(mappedFile, LibC.MADV_RANDOM);
                        }
                        result = mappedFile.appendMessages(messageExtBatch, this.appendMessageCallback, putMessageContext);
                        break;
                    case MESSAGE_SIZE_EXCEEDED:
                    case PROPERTIES_SIZE_EXCEEDED:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.MESSAGE_ILLEGAL, result));
                    case UNKNOWN_ERROR:
                    default:
                        beginTimeInLock = 0;
                        return CompletableFuture.completedFuture(new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, result));
                }

                elapsedTimeInLock = this.defaultMessageStore.getSystemClock().now() - beginLockTimestamp;
                beginTimeInLock = 0;
            } finally {
                putMessageLock.unlock();
            }

            // Increase queue offset when messages are successfully written
            if (AppendMessageStatus.PUT_OK.equals(result.getStatus())) {
                this.defaultMessageStore.increaseOffset(messageExtBatch, (short) putMessageContext.getBatchSize());
            }
        } finally {
            topicQueueLock.unlock(topicQueueKey);
        }

        if (elapsedTimeInLock > 500) {
            log.warn("[NOTIFYME]putMessages in lock cost time(ms)={}, bodyLength={} AppendMessageResult={}", elapsedTimeInLock, messageExtBatch.getBody().length, result);
        }

        if (null != unlockMappedFile && this.defaultMessageStore.getMessageStoreConfig().isWarmMapedFileEnable()) {
            this.defaultMessageStore.unlockMappedFile(unlockMappedFile);
        }

        PutMessageResult putMessageResult = new PutMessageResult(PutMessageStatus.PUT_OK, result);

        // Statistics
        storeStatsService.getSinglePutMessageTopicTimesTotal(messageExtBatch.getTopic()).add(result.getMsgNum());
        storeStatsService.getSinglePutMessageTopicSizeTotal(messageExtBatch.getTopic()).add(result.getWroteBytes());

        return handleDiskFlushAndHA(putMessageResult, messageExtBatch, needAckNums, needHandleHA);
    }

    private int calcNeedAckNums(int inSyncReplicas) {
        int needAckNums = this.defaultMessageStore.getMessageStoreConfig().getInSyncReplicas();
        if (this.defaultMessageStore.getMessageStoreConfig().isEnableAutoInSyncReplicas()) {
            needAckNums = Math.min(needAckNums, inSyncReplicas);
            needAckNums = Math.max(needAckNums, this.defaultMessageStore.getMessageStoreConfig().getMinInSyncReplicas());
        }
        return needAckNums;
    }

    private boolean needHandleHA(MessageExt messageExt) {

        if (!messageExt.isWaitStoreMsgOK()) {
            /*
              No need to sync messages that special config to extra broker slaves.
              @see MessageConst.PROPERTY_WAIT_STORE_MSG_OK
             */
            return false;
        }

        if (this.defaultMessageStore.getMessageStoreConfig().isDuplicationEnable()) {
            return false;
        }

        if (BrokerRole.SYNC_MASTER != this.defaultMessageStore.getMessageStoreConfig().getBrokerRole()) {
            // No need to check ha in async or slave broker
            return false;
        }

        return true;
    }

    private CompletableFuture<PutMessageResult> handleDiskFlushAndHA(PutMessageResult putMessageResult,
                                                                     MessageExt messageExt, int needAckNums, boolean needHandleHA) {
        // 每次写完一条消息后，就会提交 flush 请求和 replica 请求
        CompletableFuture<PutMessageStatus> flushResultFuture = handleDiskFlush(putMessageResult.getAppendMessageResult(), messageExt);
        CompletableFuture<PutMessageStatus> replicaResultFuture;
        if (!needHandleHA) {
            replicaResultFuture = CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        } else {
            replicaResultFuture = handleHA(putMessageResult.getAppendMessageResult(), putMessageResult, needAckNums);
        }

        // 等待 flush 和 replica 请求完成
        return flushResultFuture.thenCombine(replicaResultFuture, (flushStatus, replicaStatus) -> {
            if (flushStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(flushStatus);
            }
            if (replicaStatus != PutMessageStatus.PUT_OK) {
                putMessageResult.setPutMessageStatus(replicaStatus);
            }
            return putMessageResult;
        });
    }

    /**
     * 消息刷盘
     *
     * @param result
     * @param messageExt
     * @return
     */
    private CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
        return this.flushManager.handleDiskFlush(result, messageExt);
    }

    /**
     * 消息高可用刷盘handleHA
     * <p>
     *  高可用的消息刷盘，只有在设置了主从同步方式为同步方式的时候，才会有后续的逻辑。逻辑就是判断主从之间的消息差偏移量是否在设置的范围内，如果是的就可以对主库进行刷盘。
     *
     * @param result
     * @param putMessageResult
     * @param needAckNums
     * @return
     */
    private CompletableFuture<PutMessageStatus> handleHA(AppendMessageResult result, PutMessageResult putMessageResult,
                                                         int needAckNums) {
        if (needAckNums >= 0 && needAckNums <= 1) {
            return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
        }

        HAService haService = this.defaultMessageStore.getHaService();

        long nextOffset = result.getWroteOffset() + result.getWroteBytes();

        // Wait enough acks from different slaves
        GroupCommitRequest request = new GroupCommitRequest(nextOffset, this.defaultMessageStore.getMessageStoreConfig().getSlaveTimeout(), needAckNums);
        haService.putRequest(request);
        haService.getWaitNotifyObject().wakeupAll();
        return request.future();
    }

    /**
     * According to receive certain message or offset storage time if an error occurs, it returns -1
     */
    public long pickupStoreTimestamp(final long offset, final int size) {
        if (offset >= this.getMinOffset() && offset + size <= this.getMaxOffset()) {
            SelectMappedBufferResult result = this.getMessage(offset, size);
            if (null != result) {
                try {
                    int sysFlag = result.getByteBuffer().getInt(MessageDecoder.SYSFLAG_POSITION);
                    int bornhostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 8 : 20;
                    int msgStoreTimePos = 4 + 4 + 4 + 4 + 4 + 8 + 8 + 4 + 8 + bornhostLength;
                    return result.getByteBuffer().getLong(msgStoreTimePos);
                } finally {
                    result.release();
                }
            }
        }

        return -1;
    }

    public long getMinOffset() {
        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        if (mappedFile != null) {
            if (mappedFile.isAvailable()) {
                return mappedFile.getFileFromOffset();
            } else {
                return this.rollNextFile(mappedFile.getFileFromOffset());
            }
        }

        return -1;
    }

    public SelectMappedBufferResult getMessage(final long offset, final int size) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
        if (mappedFile != null) {
            int pos = (int) (offset % mappedFileSize);
            SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(pos, size);
            if (null != selectMappedBufferResult) {
                selectMappedBufferResult.setInCache(coldDataCheckService.isDataInPageCache(offset));
                return selectMappedBufferResult;
            }
        }
        return null;
    }

    public long rollNextFile(final long offset) {
        int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog();
        return offset + mappedFileSize - offset % mappedFileSize;
    }

    public void destroy() {
        this.mappedFileQueue.destroy();
    }

    public boolean appendData(long startOffset, byte[] data, int dataStart, int dataLength) {
        putMessageLock.lock();
        try {
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(startOffset);
            if (null == mappedFile) {
                log.error("appendData getLastMappedFile error  " + startOffset);
                return false;
            }

            return mappedFile.appendMessage(data, dataStart, dataLength);
        } finally {
            putMessageLock.unlock();
        }
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        return this.mappedFileQueue.retryDeleteFirstFile(intervalForcibly);
    }

    public void checkSelf() {
        mappedFileQueue.checkSelf();
    }

    public long lockTimeMills() {
        long diff = 0;
        long begin = this.beginTimeInLock;
        if (begin > 0) {
            diff = this.defaultMessageStore.now() - begin;
        }

        if (diff < 0) {
            diff = 0;
        }

        return diff;
    }

    protected short getMessageNum(MessageExtBrokerInner msgInner) {
        short messageNum = 1;
        // IF inner batch, build batchQueueOffset and batchNum property.
        CQType cqType = getCqType(msgInner);

        if (MessageSysFlag.check(msgInner.getSysFlag(), MessageSysFlag.INNER_BATCH_FLAG) || CQType.BatchCQ.equals(cqType)) {
            if (msgInner.getProperty(MessageConst.PROPERTY_INNER_NUM) != null) {
                messageNum = Short.parseShort(msgInner.getProperty(MessageConst.PROPERTY_INNER_NUM));
                messageNum = messageNum >= 1 ? messageNum : 1;
            }
        }

        return messageNum;
    }

    private CQType getCqType(MessageExtBrokerInner msgInner) {
        Optional<TopicConfig> topicConfig = this.defaultMessageStore.getTopicConfig(msgInner.getTopic());
        return QueueTypeUtils.getCQType(topicConfig);
    }

    /**
     * 消息服务类
     */
    abstract class FlushCommitLogService extends ServiceThread {
        protected static final int RETRY_TIMES_OVER = 10;
    }

    /**
     * 因此对于CommitRealTimeService，工作主要分2步：
     * 1.如果是使用了堆外内存，那么调用fileChannel的刷盘
     * 2.如果非堆外内存，那么调用mappedByteBuffer的刷盘
     * <p>
     * 消息提交
     * CommitRealTimeService主要就是定时的把临时存储池中的消息commit到FileChannel中，便于下次flush刷盘操作。而这个类只有在开启临时存储池的时候才会有用。
     */
    class CommitRealTimeService extends FlushCommitLogService {

        //上次commit的时间戳
        private long lastCommitTimestamp = 0;

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerIdentity().getIdentifier() + CommitRealTimeService.class.getSimpleName();
            }
            return CommitRealTimeService.class.getSimpleName();
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            while (!this.isStopped()) {
//                获取配置的 刷新commitLog频次   默认200ms
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitIntervalCommitLog();
                //获取配置的 提交数据页数  默认4
                int commitDataLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogLeastPages();
                //获取配置的 提交commitLog最大频次  默认200ms
                int commitDataThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getCommitCommitLogThoroughInterval();

                //时间滑动窗口
                long begin = System.currentTimeMillis();
                if (begin >= (this.lastCommitTimestamp + commitDataThoroughInterval)) {
                    this.lastCommitTimestamp = begin;
                    commitDataLeastPages = 0;
                }

                try {
                    //对数据进行提交
                    boolean result = CommitLog.this.mappedFileQueue.commit(commitDataLeastPages);
                    long end = System.currentTimeMillis();

                    if (!result) {
                        this.lastCommitTimestamp = end; // result = false means some data committed.
                        CommitLog.this.flushManager.wakeUpFlush();
                    }
                    CommitLog.this.getMessageStore().getPerfCounter().flowOnce("COMMIT_DATA_TIME_MS", (int) (end - begin));
                    if (end - begin > 500) {
                        log.info("Commit data to file costs {} ms", end - begin);
                    }
                    this.waitForRunning(interval);
                } catch (Throwable e) {
                    CommitLog.log.error(this.getServiceName() + " service has exception. ", e);
                }
            }

            boolean result = false;
            //进入这里表面服务准备停止，此时把还没提交的进行提交，最多重试10次
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.commit(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }
            CommitLog.log.info(this.getServiceName() + " service end");
        }
    }

    /**
     * 消息异步刷盘  FlushRealTimeService的主要作用就是刷盘相关的
     * <p>
     * 逻辑就是一直循环不断把映射文件队列中的消息进行刷盘
     */
    class FlushRealTimeService extends FlushCommitLogService {
        private long lastFlushTimestamp = 0;
        private long printTimes = 0;

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");
            //如果任务没有停止，停止的时候会调用对应的shutdown方法，把对应的stop字段修改为true
            while (!this.isStopped()) {
                //获取是否定时刷新日志的设定 参数为 flushCommitLogTimed  默认为false
                boolean flushCommitLogTimed = CommitLog.this.defaultMessageStore.getMessageStoreConfig().isFlushCommitLogTimed();
                //获取刷新到磁盘的时间间隔 参数为 flushIntervalCommitLog 默认为500毫秒
                int interval = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushIntervalCommitLog();
                //获取一次刷新到磁盘的最少页数，参数为flushCommitLogLeastPages 默认为4
                int flushPhysicQueueLeastPages = CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogLeastPages();
                //获取刷新CommitLog的频率 参数为flushCommitLogThoroughInterval 默认为10000毫秒
                int flushPhysicQueueThoroughInterval =
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushCommitLogThoroughInterval();

                boolean printFlushProgress = false;

                // Print flush progress
                long currentTimeMillis = System.currentTimeMillis();
                ////计算日志刷新进度信息
                if (currentTimeMillis >= (this.lastFlushTimestamp + flushPhysicQueueThoroughInterval)) {
                    this.lastFlushTimestamp = currentTimeMillis;
                    flushPhysicQueueLeastPages = 0;
                    printFlushProgress = (printTimes++ % 10) == 0;
                }

                try {
                    //如果定时刷新日志，则把线程sleep对应的规定时间
                    if (flushCommitLogTimed) {
                        Thread.sleep(interval);
                    } else {
                        //使用的是CountDownLatch等待对应时间
                        this.waitForRunning(interval);
                    }
                    //打印进度
                    if (printFlushProgress) {
                        this.printFlushProgress();
                    }

                    long begin = System.currentTimeMillis();
                    //进行文件的刷盘
                    CommitLog.this.mappedFileQueue.flush(flushPhysicQueueLeastPages);
                    //获取文件的最后修改时间也就是最后的刷新时间
                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
//                    设置CheckPoint文件的physicMsgTimestamp  消息物理落盘时间
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }
                    long past = System.currentTimeMillis() - begin;
                    CommitLog.this.getMessageStore().getPerfCounter().flowOnce("FLUSH_DATA_TIME_MS", (int) past);
                    if (past > 500) {
                        log.info("Flush data to disk costs {} ms", past);
                    }
                } catch (Throwable e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                    this.printFlushProgress();
                }
            }

            // Normal shutdown, to ensure that all the flush before exit
            boolean result = false;
            //如果服务停止，那么把剩余的没有刷新到磁盘的消息刷盘，重复次数为10次
            for (int i = 0; i < RETRY_TIMES_OVER && !result; i++) {
                result = CommitLog.this.mappedFileQueue.flush(0);
                CommitLog.log.info(this.getServiceName() + " service shutdown, retry " + (i + 1) + " times " + (result ? "OK" : "Not OK"));
            }

            this.printFlushProgress();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerConfig().getIdentifier() + FlushRealTimeService.class.getSimpleName();
            }
            return FlushRealTimeService.class.getSimpleName();
        }

        private void printFlushProgress() {
            // CommitLog.log.info("how much disk fall behind memory, "
            // + CommitLog.this.mappedFileQueue.howMuchFallBehind());
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }

    /**
     * 请求Request
     */
    public static class GroupCommitRequest {
        private final long nextOffset;
        // Indicate the GroupCommitRequest result: true or false
        private final CompletableFuture<PutMessageStatus> flushOKFuture = new CompletableFuture<>();
        private volatile int ackNums = 1;
        private final long deadLine;

        public GroupCommitRequest(long nextOffset, long timeoutMillis) {
            this.nextOffset = nextOffset;
            this.deadLine = System.nanoTime() + (timeoutMillis * 1_000_000);
        }

        public GroupCommitRequest(long nextOffset, long timeoutMillis, int ackNums) {
            this(nextOffset, timeoutMillis);
            this.ackNums = ackNums;
        }

        public long getNextOffset() {
            return nextOffset;
        }

        public int getAckNums() {
            return ackNums;
        }

        public long getDeadLine() {
            return deadLine;
        }

        public void wakeupCustomer(final PutMessageStatus status) {
            this.flushOKFuture.complete(status);
        }

        public CompletableFuture<PutMessageStatus> future() {
            return flushOKFuture;
        }
    }

    /**
     * 消息同步刷盘GroupCommitService
     * <p>
     * 这个类内部使用了CountDownLatch来进行一个任务调度
     * GroupCommit Service
     */
    class GroupCommitService extends FlushCommitLogService {
        private volatile LinkedList<GroupCommitRequest> requestsWrite = new LinkedList<>();
        private volatile LinkedList<GroupCommitRequest> requestsRead = new LinkedList<>();
        //自旋锁
        private final PutMessageSpinLock lock = new PutMessageSpinLock();

        public void putRequest(final GroupCommitRequest request) {
            lock.lock();
            try {
                //添加写请求到集合中
                this.requestsWrite.add(request);
            } finally {
                lock.unlock();
            }
            //启动提交线程
            this.wakeup();
        }

        /**
         * 交换请求
         */
        private void swapRequests() {
            lock.lock();
            try {
                LinkedList<GroupCommitRequest> tmp = this.requestsWrite;
                this.requestsWrite = this.requestsRead;
                this.requestsRead = tmp;
            } finally {
                lock.unlock();
            }
        }

        private void doCommit() {
            //如果读任务不为空则迭代处理
            if (!this.requestsRead.isEmpty()) {

                for (GroupCommitRequest req : this.requestsRead) {
                    // There may be a message in the next file, so a maximum of
                    // two times the flush
                    //可能存在一条消息存在下一个文件中，因此最多可能存在两次刷盘
                    boolean flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                    for (int i = 0; i < 2 && !flushOK; i++) {
                        CommitLog.this.mappedFileQueue.flush(0);
                        //如果文件刷盘的偏移量<请求的下一个偏移量，则说明还没有刷新完，还需要继续刷新
                        flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                    }
                    ////刷新完毕 唤醒用户线程
                    req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                }

                long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                if (storeTimestamp > 0) {
                    //刷新CheckPoint文件的physicMsgTimestamp  消息物理落盘时间
                    CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                }

                this.requestsRead = new LinkedList<>();
            } else {
                // Because of individual messages is set to not sync flush, it
                // will come to this process
                CommitLog.this.mappedFileQueue.flush(0);
            }
        }

        @Override
        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            //服务没有停止则循环进行
            while (!this.isStopped()) {
                try {
                    //等待10毫秒后执行,这个里面会调用onWaitEnd 方法
                    this.waitForRunning(10);
                    //执行提交
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }
            //交换读写任务，能进入这里说明应用已经准备停止了，
            this.swapRequests();
            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        /**
         * 在运行的时候会先等待10毫秒，而这10毫秒期间会调用，内部的onWaitEnd方法进而调用swapRequests方法，吧读写请求进行交换。
         */
        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerConfig().getIdentifier() + GroupCommitService.class.getSimpleName();
            }
            return GroupCommitService.class.getSimpleName();
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }

    class GroupCheckService extends FlushCommitLogService {
        private volatile List<GroupCommitRequest> requestsWrite = new ArrayList<>();
        private volatile List<GroupCommitRequest> requestsRead = new ArrayList<>();

        public boolean isAsyncRequestsFull() {
            return requestsWrite.size() > CommitLog.this.defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests() * 2;
        }

        public synchronized boolean putRequest(final GroupCommitRequest request) {
            synchronized (this.requestsWrite) {
                this.requestsWrite.add(request);
            }
            if (hasNotified.compareAndSet(false, true)) {
                waitPoint.countDown(); // notify
            }
            boolean flag = this.requestsWrite.size() >
                    CommitLog.this.defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests();
            if (flag) {
                log.info("Async requests {} exceeded the threshold {}", requestsWrite.size(),
                        CommitLog.this.defaultMessageStore.getMessageStoreConfig().getMaxAsyncPutMessageRequests());
            }

            return flag;
        }

        private void swapRequests() {
            List<GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        }

        private void doCommit() {
            synchronized (this.requestsRead) {
                if (!this.requestsRead.isEmpty()) {
                    for (GroupCommitRequest req : this.requestsRead) {
                        // There may be a message in the next file, so a maximum of
                        // two times the flush
                        boolean flushOK = false;
                        for (int i = 0; i < 1000; i++) {
                            flushOK = CommitLog.this.mappedFileQueue.getFlushedWhere() >= req.getNextOffset();
                            if (flushOK) {
                                break;
                            } else {
                                try {
                                    Thread.sleep(1);
                                } catch (Throwable ignored) {

                                }
                            }
                        }
                        req.wakeupCustomer(flushOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }

                    long storeTimestamp = CommitLog.this.mappedFileQueue.getStoreTimestamp();
                    if (storeTimestamp > 0) {
                        CommitLog.this.defaultMessageStore.getStoreCheckpoint().setPhysicMsgTimestamp(storeTimestamp);
                    }

                    this.requestsRead.clear();
                }
            }
        }

        public void run() {
            CommitLog.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.waitForRunning(1);
                    this.doCommit();
                } catch (Exception e) {
                    CommitLog.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // Under normal circumstances shutdown, wait for the arrival of the
            // request, and then flush
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                CommitLog.log.warn("GroupCommitService Exception, ", e);
            }

            synchronized (this) {
                this.swapRequests();
            }

            this.doCommit();

            CommitLog.log.info(this.getServiceName() + " service end");
        }

        @Override
        protected void onWaitEnd() {
            this.swapRequests();
        }

        @Override
        public String getServiceName() {
            if (CommitLog.this.defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return CommitLog.this.defaultMessageStore.getBrokerConfig().getIdentifier() + GroupCheckService.class.getSimpleName();
            }
            return GroupCheckService.class.getSimpleName();
        }

        @Override
        public long getJoinTime() {
            return 1000 * 60 * 5;
        }
    }

    /**
     * 消息回调
     */
    class DefaultAppendMessageCallback implements AppendMessageCallback {
        // File at the end of the minimum fixed length empty
        private static final int END_FILE_MIN_BLANK_LENGTH = 4 + 4;
        // Store the message content
        private final ByteBuffer msgStoreItemMemory;

        DefaultAppendMessageCallback() {
            this.msgStoreItemMemory = ByteBuffer.allocate(END_FILE_MIN_BLANK_LENGTH);
        }

        /**
         * 1.将消息编码
         * 2.将编码后的消息写入buffer中，可以是writerBuffer或者mappedByteBuffer
         * <p>
         * 此时虽然字节流已经写入了buffer中，但是对于堆外内存，此时数据还仅存在于内存中，而对于mappedByteBuffer，虽然会有系统线程定时刷数据落盘，
         * 但是这并非我们可以控，因此也只能假设还未落盘。为了保证数据能落盘，rocketmq还有一个异步刷盘的线程，接下去再来看下异步刷盘是如何处理的。
         * 查看CommitLog的构造函数，其中有3个service，分别负责同步刷盘、异步刷盘和堆外内存写入fileChann
         *
         * @param fileFromOffset
         * @param byteBuffer
         * @param maxBlank
         * @param msgInner
         * @param putMessageContext
         * @return
         */
        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBrokerInner msgInner, PutMessageContext putMessageContext) {
            // STORETIMESTAMP + STOREHOSTADDRESS + OFFSET <br>
            //文件的物理偏移量
            // PHY OFFSET
            long wroteOffset = fileFromOffset + byteBuffer.position();
            //创建msgId，底层存储由16个字节表示。
            //创建全局唯一消息ID，消息ID有16字节

            Supplier<String> msgIdSupplier = () -> {
                int sysflag = msgInner.getSysFlag();
                //4字节IP,4字节端口号，8字节消息偏移量
                int msgIdLen = (sysflag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 + 8 : 16 + 4 + 8;
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(msgInner.getStoreHost(), msgIdBuffer);
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer
                msgIdBuffer.putLong(msgIdLen - 8, wroteOffset);
                //为了消息ID具备可读性，返回给应用程序的msgId为字符类型，可
                //以通过UtilAll. bytes2string方法将msgId字节数组转换成字符串，
                //通过UtilAll.string2bytes方法将msgId字符串还原成16字节的数组，
                //根据提取的消息物理偏移量，可以快速通过msgId找到消息内容
                return UtilAll.bytes2string(msgIdBuffer.array());
            };

            // Record ConsumeQueue information
            Long queueOffset = msgInner.getQueueOffset();

            // this msg maybe a inner-batch msg.
            short messageNum = getMessageNum(msgInner);

            // Transaction messages that require special handling
            final int tranType = MessageSysFlag.getTransactionValue(msgInner.getSysFlag());
            switch (tranType) {
                // Prepared and Rollback message is not consumed, will not enter the consume queue
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    queueOffset = 0L;
                    break;
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                default:
                    break;
            }

            ByteBuffer preEncodeBuffer = msgInner.getEncodedBuff();
            final int msgLen = preEncodeBuffer.getInt(0);

            // Determines whether there is sufficient free space
            if ((msgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                this.msgStoreItemMemory.clear();
                // 1 TOTALSIZE
                this.msgStoreItemMemory.putInt(maxBlank);
                // 2 MAGICCODE
                this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                // 3 The remaining space may be any value
                // Here the length of the specially set maxBlank
                final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
                byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset,
                        maxBlank, /* only wrote 8 bytes, but declare wrote maxBlank for compute write position */
                        msgIdSupplier, msgInner.getStoreTimestamp(),
                        queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            }

            int pos = 4 + 4 + 4 + 4 + 4;
            // 6 QUEUEOFFSET
            preEncodeBuffer.putLong(pos, queueOffset);
            pos += 8;
            // 7 PHYSICALOFFSET
            preEncodeBuffer.putLong(pos, fileFromOffset + byteBuffer.position());
            int ipLen = (msgInner.getSysFlag() & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
            pos += 8 + 4 + 8 + ipLen;
            // refresh store time stamp in lock
            preEncodeBuffer.putLong(pos, msgInner.getStoreTimestamp());

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            CommitLog.this.getMessageStore().getPerfCounter().startTick("WRITE_MEMORY_TIME_MS");
            //写入buffer中，如果启用了对外内存，那么就会写入外部传入的writerBuffer，否则直接写入mappedByteBuffer中
            // Write messages to the queue buffer
            byteBuffer.put(preEncodeBuffer);
            CommitLog.this.getMessageStore().getPerfCounter().endTick("WRITE_MEMORY_TIME_MS");
            msgInner.setEncodedBuff(null);
            return new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, msgLen, msgIdSupplier,
                    msgInner.getStoreTimestamp(), queueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills, messageNum);
        }

        public AppendMessageResult doAppend(final long fileFromOffset, final ByteBuffer byteBuffer, final int maxBlank,
                                            final MessageExtBatch messageExtBatch, PutMessageContext putMessageContext) {
            byteBuffer.mark();
            //physical offset
            long wroteOffset = fileFromOffset + byteBuffer.position();
            // Record ConsumeQueue information
            Long queueOffset = messageExtBatch.getQueueOffset();
            long beginQueueOffset = queueOffset;
            int totalMsgLen = 0;
            int msgNum = 0;

            final long beginTimeMills = CommitLog.this.defaultMessageStore.now();
            ByteBuffer messagesByteBuff = messageExtBatch.getEncodedBuff();

            int sysFlag = messageExtBatch.getSysFlag();
            int bornHostLength = (sysFlag & MessageSysFlag.BORNHOST_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            int storeHostLength = (sysFlag & MessageSysFlag.STOREHOSTADDRESS_V6_FLAG) == 0 ? 4 + 4 : 16 + 4;
            Supplier<String> msgIdSupplier = () -> {
                int msgIdLen = storeHostLength + 8;
                int batchCount = putMessageContext.getBatchSize();
                long[] phyPosArray = putMessageContext.getPhyPos();
                ByteBuffer msgIdBuffer = ByteBuffer.allocate(msgIdLen);
                MessageExt.socketAddress2ByteBuffer(messageExtBatch.getStoreHost(), msgIdBuffer);
                msgIdBuffer.clear();//because socketAddress2ByteBuffer flip the buffer

                StringBuilder buffer = new StringBuilder(batchCount * msgIdLen * 2 + batchCount - 1);
                for (int i = 0; i < phyPosArray.length; i++) {
                    msgIdBuffer.putLong(msgIdLen - 8, phyPosArray[i]);
                    String msgId = UtilAll.bytes2string(msgIdBuffer.array());
                    if (i != 0) {
                        buffer.append(',');
                    }
                    buffer.append(msgId);
                }
                return buffer.toString();
            };

            messagesByteBuff.mark();
            int index = 0;
            while (messagesByteBuff.hasRemaining()) {
                // 1 TOTALSIZE
                final int msgPos = messagesByteBuff.position();
                final int msgLen = messagesByteBuff.getInt();

                totalMsgLen += msgLen;
                // Determines whether there is sufficient free space
                if ((totalMsgLen + END_FILE_MIN_BLANK_LENGTH) > maxBlank) {
                    this.msgStoreItemMemory.clear();
                    // 1 TOTALSIZE
                    this.msgStoreItemMemory.putInt(maxBlank);
                    // 2 MAGICCODE
                    this.msgStoreItemMemory.putInt(CommitLog.BLANK_MAGIC_CODE);
                    // 3 The remaining space may be any value
                    //ignore previous read
                    messagesByteBuff.reset();
                    // Here the length of the specially set maxBlank
                    byteBuffer.reset(); //ignore the previous appended messages
                    byteBuffer.put(this.msgStoreItemMemory.array(), 0, 8);
                    return new AppendMessageResult(AppendMessageStatus.END_OF_FILE, wroteOffset, maxBlank, msgIdSupplier, messageExtBatch.getStoreTimestamp(),
                            beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
                }
                //move to add queue offset and commitlog offset
                int pos = msgPos + 20;
                messagesByteBuff.putLong(pos, queueOffset);
                pos += 8;
                messagesByteBuff.putLong(pos, wroteOffset + totalMsgLen - msgLen);
                // 8 SYSFLAG, 9 BORNTIMESTAMP, 10 BORNHOST, 11 STORETIMESTAMP
                pos += 8 + 4 + 8 + bornHostLength;
                // refresh store time stamp in lock
                messagesByteBuff.putLong(pos, messageExtBatch.getStoreTimestamp());

                putMessageContext.getPhyPos()[index++] = wroteOffset + totalMsgLen - msgLen;
                queueOffset++;
                msgNum++;
                messagesByteBuff.position(msgPos + msgLen);
            }

            messagesByteBuff.position(0);
            messagesByteBuff.limit(totalMsgLen);
            byteBuffer.put(messagesByteBuff);
            messageExtBatch.setEncodedBuff(null);
            AppendMessageResult result = new AppendMessageResult(AppendMessageStatus.PUT_OK, wroteOffset, totalMsgLen, msgIdSupplier,
                    messageExtBatch.getStoreTimestamp(), beginQueueOffset, CommitLog.this.defaultMessageStore.now() - beginTimeMills);
            result.setMsgNum(msgNum);

            return result;
        }

    }

    /**
     * 刷盘管理器的默认实现
     */
    class DefaultFlushManager implements FlushManager {
        //刷盘的任务类
        private final FlushCommitLogService flushCommitLogService;

        //If TransientStorePool enabled, we must flush message to FileChannel at fixed periods
        private final FlushCommitLogService commitRealTimeService;

        public DefaultFlushManager() {
            //同步刷盘
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                this.flushCommitLogService = new CommitLog.GroupCommitService();
            } else {
                //异步刷盘
                this.flushCommitLogService = new CommitLog.FlushRealTimeService();
            }

            //将对外内存的数据写入fileChannel
            this.commitRealTimeService = new CommitLog.CommitRealTimeService();
        }

        /**
         * 有的是根据构造CommitLog类的时候进行初始化的。而对应的启动和停止在CommitLog中，而这些方法的调用又是在前面字段属性介绍的DefaultMessageStore中进行调用的
         */
        @Override
        public void start() {
            // 开启刷盘线程
            this.flushCommitLogService.start();
            /**
             *   如果使用的是临时存储池来保存消息，则启动定期提交消息的线程，把存储池的信息提交到fileChannel中
             *   只有在开启了使用临时存储池 && 刷盘为异步刷盘 && 是master节点  的情况才会为true
             */

            if (defaultMessageStore.isTransientStorePoolEnable()) {
                this.commitRealTimeService.start();
            }
        }

        public void handleDiskFlush(AppendMessageResult result, PutMessageResult putMessageResult,
                                    MessageExt messageExt) {
            // Synchronization flush
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
                if (messageExt.isWaitStoreMsgOK()) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    service.putRequest(request);
                    CompletableFuture<PutMessageStatus> flushOkFuture = request.future();
                    PutMessageStatus flushStatus = null;
                    try {
                        flushStatus = flushOkFuture.get(CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout(), TimeUnit.MILLISECONDS);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        //flushOK=false;
                    }
                    if (flushStatus != PutMessageStatus.PUT_OK) {
                        log.error("do groupcommit, wait for flush failed, topic: " + messageExt.getTopic() + " tags: " + messageExt.getTags() + " client address: " + messageExt.getBornHostString());
                        putMessageResult.setPutMessageStatus(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    }
                } else {
                    service.wakeup();
                }
            }
            // Asynchronous flush
            else {
                if (!CommitLog.this.defaultMessageStore.isTransientStorePoolEnable()) {
                    flushCommitLogService.wakeup();
                } else {
                    commitRealTimeService.wakeup();
                }
            }
        }

        @Override
        public CompletableFuture<PutMessageStatus> handleDiskFlush(AppendMessageResult result, MessageExt messageExt) {
            // Synchronization flush 同步刷新
            // Synchronization flush
            if (FlushDiskType.SYNC_FLUSH == CommitLog.this.defaultMessageStore.getMessageStoreConfig().getFlushDiskType()) {
                final GroupCommitService service = (GroupCommitService) this.flushCommitLogService;
                //是否等待存储
                if (messageExt.isWaitStoreMsgOK()) {
                    GroupCommitRequest request = new GroupCommitRequest(result.getWroteOffset() + result.getWroteBytes(), CommitLog.this.defaultMessageStore.getMessageStoreConfig().getSyncFlushTimeout());
                    flushDiskWatcher.add(request);
                    service.putRequest(request);
                    return request.future();
                } else {
                    //如果异步直接解除阻塞 countdownLatch.countDown()
                    service.wakeup();
                    return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
                }
            }
            // Asynchronous flush 异步刷新
            // Asynchronous flush
            else {
                //没有启用临时存储池，则直接唤醒刷盘的任务
                if (!CommitLog.this.defaultMessageStore.isTransientStorePoolEnable()) {
                    flushCommitLogService.wakeup();
                } else {
                    //如果使用临时存储池，需要先唤醒提交消息的任务
                    commitRealTimeService.wakeup();
                }
                return CompletableFuture.completedFuture(PutMessageStatus.PUT_OK);
            }
        }

        @Override
        public void wakeUpFlush() {
            // now wake up flush thread.
            flushCommitLogService.wakeup();
        }

        @Override
        public void wakeUpCommit() {
            // now wake up commit log thread.
            commitRealTimeService.wakeup();
        }

        @Override
        public void shutdown() {
            //        //关闭提交临时存储池的任务
            if (defaultMessageStore.isTransientStorePoolEnable()) {
                this.commitRealTimeService.shutdown();
            }

            //关闭刷盘线程
            this.flushCommitLogService.shutdown();
        }

    }

    public int getCommitLogSize() {
        return commitLogSize;
    }

    public MappedFileQueue getMappedFileQueue() {
        return mappedFileQueue;
    }

    public MessageStore getMessageStore() {
        return defaultMessageStore;
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        this.getMappedFileQueue().swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    public boolean isMappedFilesEmpty() {
        return this.mappedFileQueue.isMappedFilesEmpty();
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        this.getMappedFileQueue().cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    public FlushManager getFlushManager() {
        return flushManager;
    }

    private boolean isCloseReadAhead() {
        return !MixAll.isWindows() && !defaultMessageStore.getMessageStoreConfig().isDataReadAheadEnable();
    }


    /**
     * 冷数据检查服务
     */
    public class ColdDataCheckService extends ServiceThread {
        private final SystemClock systemClock = new SystemClock();
        private final ConcurrentHashMap<String, byte[]> pageCacheMap = new ConcurrentHashMap<>();
        private int pageSize = -1;
        private int sampleSteps = 32;

        public ColdDataCheckService() {
            sampleSteps = defaultMessageStore.getMessageStoreConfig().getSampleSteps();
            if (sampleSteps <= 0) {
                sampleSteps = 32;
            }
            initPageSize();
            scanFilesInPageCache();
        }

        @Override
        public String getServiceName() {
            return ColdDataCheckService.class.getSimpleName();
        }

        @Override
        public void run() {
            log.info("{} service started", this.getServiceName());
            while (!this.isStopped()) {
                try {
                    if (MixAll.isWindows() || !defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable() || !defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable()) {
                        pageCacheMap.clear();
                        this.waitForRunning(180 * 1000);
                        continue;
                    } else {
                        this.waitForRunning(defaultMessageStore.getMessageStoreConfig().getTimerColdDataCheckIntervalMs());
                    }

                    if (pageSize < 0) {
                        initPageSize();
                    }

                    long beginClockTimestamp = this.systemClock.now();
                    scanFilesInPageCache();
                    long costTime = this.systemClock.now() - beginClockTimestamp;
                    log.info("[{}] scanFilesInPageCache-cost {} ms.", costTime > 30 * 1000 ? "NOTIFYME" : "OK", costTime);
                } catch (Throwable e) {
                    log.warn(this.getServiceName() + " service has e: {}", e);
                }
            }
            log.info("{} service end", this.getServiceName());
        }

        public boolean isDataInPageCache(final long offset) {
            if (!defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                return true;
            }
            if (pageSize <= 0 || sampleSteps <= 0) {
                return true;
            }
            if (!defaultMessageStore.checkInColdAreaByCommitOffset(offset, getMaxOffset())) {
                return true;
            }
            if (!defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable()) {
                return false;
            }

            MappedFile mappedFile = mappedFileQueue.findMappedFileByOffset(offset, offset == 0);
            if (null == mappedFile) {
                return true;
            }
            byte[] bytes = pageCacheMap.get(mappedFile.getFileName());
            if (null == bytes) {
                return true;
            }

            int pos = (int) (offset % defaultMessageStore.getMessageStoreConfig().getMappedFileSizeCommitLog());
            int realIndex = pos / pageSize / sampleSteps;
            return bytes.length - 1 >= realIndex && bytes[realIndex] != 0;
        }

        private void scanFilesInPageCache() {
            if (MixAll.isWindows() || !defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable() || !defaultMessageStore.getMessageStoreConfig().isColdDataScanEnable() || pageSize <= 0) {
                return;
            }
            try {
                log.info("pageCacheMap key size: {}", pageCacheMap.size());
                clearExpireMappedFile();
                mappedFileQueue.getMappedFiles().forEach(mappedFile -> {
                    byte[] pageCacheTable = checkFileInPageCache(mappedFile);
                    if (sampleSteps > 1) {
                        pageCacheTable = sampling(pageCacheTable, sampleSteps);
                    }
                    pageCacheMap.put(mappedFile.getFileName(), pageCacheTable);
                });
            } catch (Exception e) {
                log.error("scanFilesInPageCache exception", e);
            }
        }

        private void clearExpireMappedFile() {
            Set<String> currentFileSet = mappedFileQueue.getMappedFiles().stream().map(MappedFile::getFileName).collect(Collectors.toSet());
            pageCacheMap.forEach((key, value) -> {
                if (!currentFileSet.contains(key)) {
                    pageCacheMap.remove(key);
                    log.info("clearExpireMappedFile fileName: {}, has been clear", key);
                }
            });
        }

        private byte[] sampling(byte[] pageCacheTable, int sampleStep) {
            byte[] sample = new byte[(pageCacheTable.length + sampleStep - 1) / sampleStep];
            for (int i = 0, j = 0; i < pageCacheTable.length && j < sample.length; i += sampleStep) {
                sample[j++] = pageCacheTable[i];
            }
            return sample;
        }

        private byte[] checkFileInPageCache(MappedFile mappedFile) {
            long fileSize = mappedFile.getFileSize();
            final long address = ((DirectBuffer) mappedFile.getMappedByteBuffer()).address();
            int pageNums = (int) (fileSize + this.pageSize - 1) / this.pageSize;
            byte[] pageCacheRst = new byte[pageNums];
            int mincore = LibC.INSTANCE.mincore(new Pointer(address), new NativeLong(fileSize), pageCacheRst);
            if (mincore != 0) {
                log.error("checkFileInPageCache call the LibC.INSTANCE.mincore error, fileName: {}, fileSize: {}",
                        mappedFile.getFileName(), fileSize);
                for (int i = 0; i < pageNums; i++) {
                    pageCacheRst[i] = 1;
                }
            }
            return pageCacheRst;
        }

        private void initPageSize() {
            if (pageSize < 0 && defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                try {
                    if (!MixAll.isWindows()) {
                        pageSize = LibC.INSTANCE.getpagesize();
                    } else {
                        defaultMessageStore.getMessageStoreConfig().setColdDataFlowControlEnable(false);
                        log.info("windows os, coldDataCheckEnable force setting to be false");
                    }
                    log.info("initPageSize pageSize: {}", pageSize);
                } catch (Exception e) {
                    defaultMessageStore.getMessageStoreConfig().setColdDataFlowControlEnable(false);
                    log.error("initPageSize error, coldDataCheckEnable force setting to be false ", e);
                }
            }
        }

        /**
         * this result is not high accurate.
         */
        public boolean isMsgInColdArea(String group, String topic, int queueId, long offset) {
            if (!defaultMessageStore.getMessageStoreConfig().isColdDataFlowControlEnable()) {
                return false;
            }
            try {
                ConsumeQueue consumeQueue = (ConsumeQueue) defaultMessageStore.findConsumeQueue(topic, queueId);
                if (null == consumeQueue) {
                    return false;
                }
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                if (null == bufferConsumeQueue || null == bufferConsumeQueue.getByteBuffer()) {
                    return false;
                }
                long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                return defaultMessageStore.checkInColdAreaByCommitOffset(offsetPy, getMaxOffset());
            } catch (Exception e) {
                log.error("isMsgInColdArea group: {}, topic: {}, queueId: {}, offset: {}",
                        group, topic, queueId, offset, e);
            }
            return false;
        }
    }

    public void scanFileAndSetReadMode(int mode) {
        if (MixAll.isWindows()) {
            log.info("windows os stop scanFileAndSetReadMode");
            return;
        }
        try {
            log.info("scanFileAndSetReadMode mode: {}", mode);
            mappedFileQueue.getMappedFiles().forEach(mappedFile -> {
                setFileReadMode(mappedFile, mode);
            });
        } catch (Exception e) {
            log.error("scanFileAndSetReadMode exception", e);
        }
    }

    private int setFileReadMode(MappedFile mappedFile, int mode) {
        if (null == mappedFile) {
            log.error("setFileReadMode mappedFile is null");
            return -1;
        }
        final long address = ((DirectBuffer) mappedFile.getMappedByteBuffer()).address();
        int madvise = LibC.INSTANCE.madvise(new Pointer(address), new NativeLong(mappedFile.getFileSize()), mode);
        if (madvise != 0) {
            log.error("setFileReadMode error fileName: {}, madvise: {}, mode:{}", mappedFile.getFileName(), madvise, mode);
        }
        return madvise;
    }

    public ColdDataCheckService getColdDataCheckService() {
        return coldDataCheckService;
    }
}
