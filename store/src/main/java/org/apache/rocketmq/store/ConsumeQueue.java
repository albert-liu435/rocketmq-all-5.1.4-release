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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.attribute.CQType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.logfile.MappedFile;
import org.apache.rocketmq.store.queue.ConsumeQueueInterface;
import org.apache.rocketmq.store.queue.CqUnit;
import org.apache.rocketmq.store.queue.FileQueueLifeCycle;
import org.apache.rocketmq.store.queue.QueueOffsetOperator;
import org.apache.rocketmq.store.queue.ReferredIterator;
import org.apache.rocketmq.store.timer.TimerMessageStore;

/**
 * consumequeue文件由Consumequeue类提供服务（大文件）
 * <p>
 * 消息消费队列，引入的目的主要是提高消息消费的性能，由于RocketMQ是基于主题topic的订阅模式，消息消费是针对主题进行的，如果要遍历commitlog文件中根据topic检索消息是非常低效的。
 * <p>
 * Consumer即可根据ConsumeQueue来查找待消费的消息。其中，ConsumeQueue（逻辑消费队列）作为消费消息的索引，保存了指定Topic下的队列消息在CommitLog中的起始物理偏移量offset，消息大小size和消息Tag的HashCode值。
 *  consumequeue文件可以看成是基于topic的commitlog索引文件，故consumequeue文件夹的组织方式如下：topic/queue/file三层组织结构，具体存储路径为：$HOME/store/consumequeue/{topic}/{queueId}/{fileName}。
 *  同样consumequeue文件采取定长设计，每一个条目共20个字节，分别为8字节的commitlog物理偏移量、4字节的消息长度、8字节tag hashcode，单个文件由30W个条目组成，可以像数组一样随机访问每一个条目，每个ConsumeQueue文件大小约5.72M
 * <p>
 * 消息消费者Consumer可根据ConsumeQueue来查找待消费的消息。其中，ConsumeQueue（逻辑消费队列）作为消费消息的索引，保存了指定Topic下的队列消息在CommitLog（物理消费队列）中的起始物理偏移量offset，消息大小size和消息Tag的HashCode值
 * ————————————————
 * 版权声明：本文为CSDN博主「szhlcy」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
 * 原文链接：https://blog.csdn.net/szhlcy/article/details/115146311
 * <p>
 * <p>
 * <p>
 * <p>
 * <p>
 * <p>
 * 每个ConsumeQueue都有一个id，id 的值为0到TopicConfig配置的队列数量。比如某个Topic的消费队列数量为4，那么四个ConsumeQueue的id就分别为0、1、2、3。
 * <p>
 * ConsumeQueue是不负责存储消息的，只是负责记录它所属Topic的消息在CommitLog中的偏移量，这样当消费者从Broker拉取消息的时候，就可以快速根据偏移量定位到消息。
 * <p>
 * ConsumeQueue本身同样是利用MappedFileQueue进行记录偏移量信息的
 */
public class ConsumeQueue implements ConsumeQueueInterface, FileQueueLifeCycle {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    /**
     * 队列存储的大小，分别为 CommitLog物理偏移量+body大小+tag的hashcode=20字节
     * 同样consumequeue文件采取定长设计，每一个条目共20个字节，分别为8字节的commitlog物理偏移量、4字节的消息长度、8字节tag hashcode，单个文件由30W个条目组成，
     * 可以像数组一样随机访问每一个条目，每个ConsumeQueue文件大小约5.72M
     * ConsumeQueue's store unit. Format:
     * <pre>
     * ┌───────────────────────────────┬───────────────────┬───────────────────────────────┐
     * │    CommitLog Physical Offset  │      Body Size    │            Tag HashCode       │
     * │          (8 Bytes)            │      (4 Bytes)    │             (8 Bytes)         │
     * ├───────────────────────────────┴───────────────────┴───────────────────────────────┤
     * │                                     Store Unit                                    │
     * │                                                                                   │
     * </pre>
     * ConsumeQueue's store unit. Size: CommitLog Physical Offset(8) + Body Size(4) + Tag HashCode(8) = 20 Bytes
     */
    public static final int CQ_STORE_UNIT_SIZE = 20;
    public static final int MSG_TAG_OFFSET_INDEX = 12;
    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    private final MessageStore messageStore;
    //映射文件队列
    private final MappedFileQueue mappedFileQueue;
    //消息的Topic
    private final String topic;
    //消息的queueId
    private final int queueId;
    //指定大小的缓冲，因为一个记录的大小是20byte的固定大小
    private final ByteBuffer byteBufferIndex;
    //保存的路径
    private final String storePath;
    //映射文件的大小
    private final int mappedFileSize;
    //最后一个消息对应的物理偏移量  也就是在CommitLog中的偏移量
    private long maxPhysicOffset = -1;

    /**
     * Minimum offset of the consume file queue that points to valid commit log record.
     */
    //最小的逻辑偏移量 在ConsumeQueue中的最小偏移量
    private volatile long minLogicOffset = 0;
    //ConsumeQueue的扩展文件，保存一些不重要的信息，比如消息存储时间等
    private ConsumeQueueExt consumeQueueExt = null;

    public ConsumeQueue(
            final String topic,
            final int queueId,
            final String storePath,
            final int mappedFileSize,
            final MessageStore messageStore) {
        //指定文件的存储位置
        this.storePath = storePath;
        //指定文件大小
        this.mappedFileSize = mappedFileSize;
        //指定DefaultMessageStore对象
        this.messageStore = messageStore;
        //存储指定topic消息
        this.topic = topic;
        //指定指定queueId消息
        this.queueId = queueId;
        //设置对应的文件路径，$HOME/store/consumequeue/{topic}/{queueId}/{fileName}
        String queueDir = this.storePath
                + File.separator + topic
                + File.separator + queueId;
        //创建文件映射队列
        this.mappedFileQueue = new MappedFileQueue(queueDir, mappedFileSize, null);
        //创建20个字节大小的缓冲
        this.byteBufferIndex = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        //是否启用消息队列的扩展存储
        if (messageStore.getMessageStoreConfig().isEnableConsumeQueueExt()) {
            //创建一个扩展存储对象
            this.consumeQueueExt = new ConsumeQueueExt(
                    topic,
                    queueId,
                    //consumeQueueExt的存储地址
                    StorePathConfigHelper.getStorePathConsumeQueueExt(messageStore.getMessageStoreConfig().getStorePathRootDir()),
                    //设置消费队列文件扩展大小  默认48M
                    messageStore.getMessageStoreConfig().getMappedFileSizeConsumeQueueExt(),
                    //位图过滤的位图长度
                    messageStore.getMessageStoreConfig().getBitMapLengthConsumeQueueExt()
            );
        }
    }

    /**
     * load方法调用也是在RocketMQ的Broker启动的时候，会调用到，用来加载机器内存中的ConsumeQueue文件
     *
     * @return
     */
    @Override
    public boolean load() {
        //从映射文件队列加载
        boolean result = this.mappedFileQueue.load();
        log.info("load consume queue " + this.topic + "-" + this.queueId + " " + (result ? "OK" : "Failed"));
        //存在扩展存储则加载
        if (isExtReadEnable()) {
            //消息队列扩展加载
            result &= this.consumeQueueExt.load();
        }
        return result;
    }

    /**
     * RocketMQ在启动时候，会去尝试恢复服务器中的ConsumeQueue文件。文件恢复的逻辑就是通过检查每个消息记录单元中记录信息来判断这个记录是否完整，
     * 进而分析整个文件是不是完整，最后对文件中损坏的记录进行截断。整体的恢复逻辑有点长。这里对每个消息单元的分析是基于单个消息单元的长度是20个字节长度的原理来进行分析。
     * ————————————————
     * 版权声明：本文为CSDN博主「szhlcy」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
     * 原文链接：https://blog.csdn.net/szhlcy/article/details/115146311
     */
    @Override
    public void recover() {
        final List<MappedFile> mappedFiles = this.mappedFileQueue.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            //如果文件列表大于3就从倒数第3个开始，否则从第一个开始
            int index = mappedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }
            //获取consumeQueue单个文件的大小
            int mappedFileSizeLogics = this.mappedFileSize;
            //获取最后一个映射文件
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            //映射文件处理的起始偏移量
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            long maxExtAddr = 1;
            while (true) {
                //遍历文件列表
                for (int i = 0; i < mappedFileSizeLogics; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();
                    //顺序解析，每个数据单元隔20个字节,如果offset跟size大于0则表示有效
                    if (offset >= 0 && size > 0) {
                        //正常数据的大小
                        mappedFileOffset = i + CQ_STORE_UNIT_SIZE;
                        //设置最大的物理偏移量
                        this.setMaxPhysicOffset(offset + size);
                        if (isExtAddr(tagsCode)) {
                            maxExtAddr = tagsCode;
                        }
                    } else {
                        log.info("recover current consume queue file over,  " + mappedFile.getFileName() + " "
                                + offset + " " + size + " " + tagsCode);
                        break;
                    }
                }
                //如果已经   加载正常数据的大小 = 队列文件的大小，则表示这个文件加载完毕
                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++;
                    if (index >= mappedFiles.size()) {

                        log.info("recover last consume queue file over, last mapped file "
                                + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next consume queue file, " + mappedFile.getFileName());
                    }
                } else {
                    log.info("recover current consume queue over " + mappedFile.getFileName() + " "
                            + (processOffset + mappedFileOffset));
                    break;
                }
            }
            // 完整的偏移量 = 最后一个文件的起始偏移量(getFileFromOffset) +  正常数据的长度(mappedFileOffset)
            processOffset += mappedFileOffset;
            //设置刷新到的 offset位置
            this.mappedFileQueue.setFlushedWhere(processOffset);
            //设置提交到的 offset位置
            this.mappedFileQueue.setCommittedWhere(processOffset);
            //删除有效的 offset 之后的文件
            this.mappedFileQueue.truncateDirtyFiles(processOffset);
            //如果有扩展文件，则恢复扩展文件
            if (isExtReadEnable()) {
                this.consumeQueueExt.recover();
                log.info("Truncate consume queue extend file by max {}", maxExtAddr);
                //映射文件队列删除最大offset的脏数据文件=》
                this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
            }
        }
    }

    @Override
    public long getTotalSize() {
        long totalSize = this.mappedFileQueue.getTotalFileSize();
        if (isExtReadEnable()) {
            totalSize += this.consumeQueueExt.getTotalSize();
        }
        return totalSize;
    }

    @Override
    public int getUnitSize() {
        return CQ_STORE_UNIT_SIZE;
    }

    /**
     * 根据时间获取消息在队列中的逻辑位置getOffsetInQueueByTime
     *
     * @param timestamp timestamp
     * @return
     */
    @Deprecated
    @Override
    public long getOffsetInQueueByTime(final long timestamp) {
        //根据时间找到映射的文件，文件可以知道最后一次修改的时间
        MappedFile mappedFile = this.mappedFileQueue.getConsumeQueueMappedFileByTime(timestamp,
                messageStore.getCommitLog(), BoundaryType.LOWER);
        return binarySearchInQueueByTime(mappedFile, timestamp, BoundaryType.LOWER);
    }

    @Override
    public long getOffsetInQueueByTime(final long timestamp, final BoundaryType boundaryType) {
        MappedFile mappedFile = this.mappedFileQueue.getConsumeQueueMappedFileByTime(timestamp,
                messageStore.getCommitLog(), boundaryType);
        return binarySearchInQueueByTime(mappedFile, timestamp, boundaryType);
    }

    /**
     * 统括二分查找方法进行查找
     *
     * @param mappedFile
     * @param timestamp
     * @param boundaryType
     * @return
     */
    private long binarySearchInQueueByTime(final MappedFile mappedFile, final long timestamp,
                                           BoundaryType boundaryType) {
        if (mappedFile != null) {
            long offset = 0;
            //如果文件的最小偏移量 大于 查找的时间戳所在的文件的起始偏移量 说明对应的消息在这个文件中。
            int low = minLogicOffset > mappedFile.getFileFromOffset() ? (int) (minLogicOffset - mappedFile.getFileFromOffset()) : 0;
            int high = 0;
            int midOffset = -1, targetOffset = -1, leftOffset = -1, rightOffset = -1;
            //获取最小的物理偏移量  也就是CommitLog的最小偏移量
            long minPhysicOffset = this.messageStore.getMinPhyOffset();
            int range = mappedFile.getFileSize();
            if (mappedFile.getWrotePosition() != 0 && mappedFile.getWrotePosition() != mappedFile.getFileSize()) {
                // mappedFile is the last one and is currently being written.
                range = mappedFile.getWrotePosition();
            }
            //获取文件的内容buffer
            SelectMappedBufferResult sbr = mappedFile.selectMappedBuffer(0, range);
            if (null != sbr) {
                ByteBuffer byteBuffer = sbr.getByteBuffer();
                //计算文件的最大的数据单元的偏移量
                int ceiling = byteBuffer.limit() - CQ_STORE_UNIT_SIZE;
                int floor = low;
                high = ceiling;
                try {
                    // Handle the following corner cases first:
                    // 1. store time of (high) < timestamp
                    // 2. store time of (low) > timestamp
                    long storeTime;
                    long phyOffset;
                    int size;
                    // Handle case 1
                    byteBuffer.position(ceiling);
                    phyOffset = byteBuffer.getLong();
                    size = byteBuffer.getInt();
                    storeTime = messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);

                    if (storeTime < timestamp) {
                        switch (boundaryType) {
                            case LOWER:
                                return (mappedFile.getFileFromOffset() + ceiling + CQ_STORE_UNIT_SIZE) / CQ_STORE_UNIT_SIZE;
                            case UPPER:
                                return (mappedFile.getFileFromOffset() + ceiling) / CQ_STORE_UNIT_SIZE;
                            default:
                                log.warn("Unknown boundary type");
                                break;
                        }
                    }

                    // Handle case 2
                    byteBuffer.position(floor);
                    phyOffset = byteBuffer.getLong();
                    size = byteBuffer.getInt();
                    storeTime = messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                    if (storeTime > timestamp) {
                        switch (boundaryType) {
                            case LOWER:
                                return mappedFile.getFileFromOffset() / CQ_STORE_UNIT_SIZE;
                            case UPPER:
                                return 0;
                            default:
                                log.warn("Unknown boundary type");
                                break;
                        }
                    }

                    // Perform binary search
                    //用二分法来获取更新的时间戳
                    while (high >= low) {
                        midOffset = (low + high) / (2 * CQ_STORE_UNIT_SIZE) * CQ_STORE_UNIT_SIZE;
                        byteBuffer.position(midOffset);
                        //获取消息的物理偏移量，也就是在commitLog上的偏移量
                        phyOffset = byteBuffer.getLong();
                        size = byteBuffer.getInt();
                        //如果小于最小的物理偏移量，则取下一条消息的位置
                        if (phyOffset < minPhysicOffset) {
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                            continue;
                        }
                        //按物理offset从commitLog中获取存储时间=》
                        storeTime = this.messageStore.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                        if (storeTime < 0) {
                            log.warn("Failed to query store timestamp for commit log offset: {}", phyOffset);
                            return 0;
                            ////如果存储时间相等就是要找的
                        } else if (storeTime == timestamp) {
                            targetOffset = midOffset;
                            break;
                            //如果存储时间大于目标时间，则消息需要往前找
                        } else if (storeTime > timestamp) {
                            high = midOffset - CQ_STORE_UNIT_SIZE;
                            rightOffset = midOffset;
                        } else {
                            //如果存储时间小于目标时间，则消息需要往后
                            low = midOffset + CQ_STORE_UNIT_SIZE;
                            leftOffset = midOffset;
                        }
                    }
                    //找到了符合条件的消息的逻辑地址
                    if (targetOffset != -1) {
                        // We just found ONE matched record. These next to it might also share the same store-timestamp.
                        offset = targetOffset;
                        switch (boundaryType) {
                            case LOWER: {
                                int previousAttempt = targetOffset;
                                while (true) {
                                    int attempt = previousAttempt - CQ_STORE_UNIT_SIZE;
                                    if (attempt < floor) {
                                        break;
                                    }
                                    byteBuffer.position(attempt);
                                    long physicalOffset = byteBuffer.getLong();
                                    int messageSize = byteBuffer.getInt();
                                    long messageStoreTimestamp = messageStore.getCommitLog()
                                            .pickupStoreTimestamp(physicalOffset, messageSize);
                                    if (messageStoreTimestamp == timestamp) {
                                        previousAttempt = attempt;
                                        continue;
                                    }
                                    break;
                                }
                                offset = previousAttempt;
                                break;
                            }
                            case UPPER: {
                                int previousAttempt = targetOffset;
                                while (true) {
                                    int attempt = previousAttempt + CQ_STORE_UNIT_SIZE;
                                    if (attempt > ceiling) {
                                        break;
                                    }
                                    byteBuffer.position(attempt);
                                    long physicalOffset = byteBuffer.getLong();
                                    int messageSize = byteBuffer.getInt();
                                    long messageStoreTimestamp = messageStore.getCommitLog()
                                            .pickupStoreTimestamp(physicalOffset, messageSize);
                                    if (messageStoreTimestamp == timestamp) {
                                        previousAttempt = attempt;
                                        continue;
                                    }
                                    break;
                                }
                                offset = previousAttempt;
                                break;
                            }
                            default: {
                                log.warn("Unknown boundary type");
                                break;
                            }
                        }
                    } else {
                        // Given timestamp does not have any message records. But we have a range enclosing the
                        // timestamp.
                        /*
                         * Consider the follow case: t2 has no consume queue entry and we are searching offset of
                         * t2 for lower and upper boundaries.
                         *  --------------------------
                         *   timestamp   Consume Queue
                         *       t1          1
                         *       t1          2
                         *       t1          3
                         *       t3          4
                         *       t3          5
                         *   --------------------------
                         * Now, we return 3 as upper boundary of t2 and 4 as its lower boundary. It looks
                         * contradictory at first sight, but it does make sense when performing range queries.
                         */
                        switch (boundaryType) {
                            case LOWER: {
                                offset = rightOffset;
                                break;
                            }

                            case UPPER: {
                                offset = leftOffset;
                                break;
                            }
                            default: {
                                log.warn("Unknown boundary type");
                                break;
                            }
                        }
                    }
                    //返回对应的消息
                    return (mappedFile.getFileFromOffset() + offset) / CQ_STORE_UNIT_SIZE;
                } finally {
                    sbr.release();
                }
            }
        }
        return 0;
    }

    /**
     * 截断逻辑文件
     *
     * @param phyOffset max commit log position
     */
    @Override
    public void truncateDirtyLogicFiles(long phyOffset) {
        truncateDirtyLogicFiles(phyOffset, true);
    }

    public void truncateDirtyLogicFiles(long phyOffset, boolean deleteFile) {

        int logicFileSize = this.mappedFileSize;

        this.setMaxPhysicOffset(phyOffset);
        long maxExtAddr = 1;
        boolean shouldDeleteFile = false;
        while (true) {
            //            获取映射队列中最后的映射文件=》
            MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
            if (mappedFile != null) {
                ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();

                mappedFile.setWrotePosition(0);
                mappedFile.setCommittedPosition(0);
                mappedFile.setFlushedPosition(0);
                //遍历所有的MappedFile，每次读取的间隔是20个字节（ConsumeQueue的单个数据单元大小为20字节）
                for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    long tagsCode = byteBuffer.getLong();

                    if (0 == i) {
                        //如果第一个单元的物理偏移量CommitLog Offset大于phyOffet，则直接删除最后一个文件。因为phyOffet表示的是最后一个有效的commitLog文件的起始偏移量。
                        if (offset >= phyOffset) {
                            shouldDeleteFile = true;
                            break;
                        } else {
                            //设置wrotePostion和CommittedPosition两个变量为解析到的数据块位置
                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.setMaxPhysicOffset(offset + size);
                            // This maybe not take effect, when not every consume queue has extend file.
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }
                        }
                    } else {
                        //解析到数据块的大小为空或者物理偏移值大于了processOffset为止。
                        if (offset >= 0 && size > 0) {

                            if (offset >= phyOffset) {
                                return;
                            }

                            int pos = i + CQ_STORE_UNIT_SIZE;
                            mappedFile.setWrotePosition(pos);
                            mappedFile.setCommittedPosition(pos);
                            mappedFile.setFlushedPosition(pos);
                            this.setMaxPhysicOffset(offset + size);
                            if (isExtAddr(tagsCode)) {
                                maxExtAddr = tagsCode;
                            }

                            if (pos == logicFileSize) {
                                return;
                            }
                        } else {
                            return;
                        }
                    }
                }

                if (shouldDeleteFile) {
                    if (deleteFile) {
                        this.mappedFileQueue.deleteLastMappedFile();
                    } else {
                        this.mappedFileQueue.deleteExpiredFile(Collections.singletonList(this.mappedFileQueue.getLastMappedFile()));
                    }
                }

            } else {
                break;
            }
        }

        if (isExtReadEnable()) {
            //            删除最大位置的消息队列=》
            this.consumeQueueExt.truncateByMaxAddress(maxExtAddr);
        }
    }

    @Override
    public long getLastOffset() {
        long lastOffset = -1;

        int logicFileSize = this.mappedFileSize;

        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile();
        if (mappedFile != null) {

            int position = mappedFile.getWrotePosition() - CQ_STORE_UNIT_SIZE;
            if (position < 0)
                position = 0;

            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            byteBuffer.position(position);
            for (int i = 0; i < logicFileSize; i += CQ_STORE_UNIT_SIZE) {
                long offset = byteBuffer.getLong();
                int size = byteBuffer.getInt();
                byteBuffer.getLong();

                if (offset >= 0 && size > 0) {
                    lastOffset = offset + size;
                } else {
                    break;
                }
            }
        }

        return lastOffset;
    }

    /**
     * 进行flush操作
     *
     * @param flushLeastPages the minimum number of pages to be flushed
     * @return
     */
    @Override
    public boolean flush(final int flushLeastPages) {
        boolean result = this.mappedFileQueue.flush(flushLeastPages);
        if (isExtReadEnable()) {
            result = result & this.consumeQueueExt.flush(flushLeastPages);
        }

        return result;
    }

    @Override
    public int deleteExpiredFile(long offset) {
        int cnt = this.mappedFileQueue.deleteExpiredFileByOffset(offset, CQ_STORE_UNIT_SIZE);
        this.correctMinOffset(offset);
        return cnt;
    }

    /**
     * Update minLogicOffset such that entries after it would point to valid commit log address.
     *
     * @param minCommitLogOffset Minimum commit log offset
     */
    @Override
    public void correctMinOffset(long minCommitLogOffset) {
        // Check if the consume queue is the state of deprecation.
        if (minLogicOffset >= mappedFileQueue.getMaxOffset()) {
            log.info("ConsumeQueue[Topic={}, queue-id={}] contains no valid entries", topic, queueId);
            return;
        }

        // Check whether the consume queue maps no valid data at all. This check may cost 1 IO operation.
        // The rationale is that consume queue always preserves the last file. In case there are many deprecated topics,
        // This check would save a lot of efforts.
        MappedFile lastMappedFile = this.mappedFileQueue.getLastMappedFile();
        if (null == lastMappedFile) {
            return;
        }

        SelectMappedBufferResult lastRecord = null;
        try {
            int maxReadablePosition = lastMappedFile.getReadPosition();
            lastRecord = lastMappedFile.selectMappedBuffer(maxReadablePosition - ConsumeQueue.CQ_STORE_UNIT_SIZE,
                    ConsumeQueue.CQ_STORE_UNIT_SIZE);
            if (null != lastRecord) {
                ByteBuffer buffer = lastRecord.getByteBuffer();
                long commitLogOffset = buffer.getLong();
                if (commitLogOffset < minCommitLogOffset) {
                    // Keep the largest known consume offset, even if this consume-queue contains no valid entries at
                    // all. Let minLogicOffset point to a future slot.
                    this.minLogicOffset = lastMappedFile.getFileFromOffset() + maxReadablePosition;
                    log.info("ConsumeQueue[topic={}, queue-id={}] contains no valid entries. Min-offset is assigned as: {}.",
                            topic, queueId, getMinOffsetInQueue());
                    return;
                }
            }
        } finally {
            if (null != lastRecord) {
                lastRecord.release();
            }
        }

        MappedFile mappedFile = this.mappedFileQueue.getFirstMappedFile();
        long minExtAddr = 1;
        if (mappedFile != null) {
            // Search from previous min logical offset. Typically, a consume queue file segment contains 300,000 entries
            // searching from previous position saves significant amount of comparisons and IOs
            boolean intact = true; // Assume previous value is still valid
            long start = this.minLogicOffset - mappedFile.getFileFromOffset();
            if (start < 0) {
                intact = false;
                start = 0;
            }

            if (start > mappedFile.getReadPosition()) {
                log.error("[Bug][InconsistentState] ConsumeQueue file {} should have been deleted",
                        mappedFile.getFileName());
                return;
            }

            SelectMappedBufferResult result = mappedFile.selectMappedBuffer((int) start);
            if (result == null) {
                log.warn("[Bug] Failed to scan consume queue entries from file on correcting min offset: {}",
                        mappedFile.getFileName());
                return;
            }

            try {
                // No valid consume entries
                if (result.getSize() == 0) {
                    log.debug("ConsumeQueue[topic={}, queue-id={}] contains no valid entries", topic, queueId);
                    return;
                }

                ByteBuffer buffer = result.getByteBuffer().slice();
                // Verify whether the previous value is still valid or not before conducting binary search
                long commitLogOffset = buffer.getLong();
                if (intact && commitLogOffset >= minCommitLogOffset) {
                    log.info("Abort correction as previous min-offset points to {}, which is greater than {}",
                            commitLogOffset, minCommitLogOffset);
                    return;
                }

                // Binary search between range [previous_min_logic_offset, first_file_from_offset + file_size)
                // Note the consume-queue deletion procedure ensures the last entry points to somewhere valid.
                int low = 0;
                int high = result.getSize() - ConsumeQueue.CQ_STORE_UNIT_SIZE;
                while (true) {
                    if (high - low <= ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        break;
                    }
                    int mid = (low + high) / 2 / ConsumeQueue.CQ_STORE_UNIT_SIZE * ConsumeQueue.CQ_STORE_UNIT_SIZE;
                    buffer.position(mid);
                    commitLogOffset = buffer.getLong();
                    if (commitLogOffset > minCommitLogOffset) {
                        high = mid;
                    } else if (commitLogOffset == minCommitLogOffset) {
                        low = mid;
                        high = mid;
                        break;
                    } else {
                        low = mid;
                    }
                }

                // Examine the last one or two entries
                for (int i = low; i <= high; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                    buffer.position(i);
                    long offsetPy = buffer.getLong();
                    buffer.position(i + 12);
                    long tagsCode = buffer.getLong();

                    if (offsetPy >= minCommitLogOffset) {
                        this.minLogicOffset = mappedFile.getFileFromOffset() + start + i;
                        log.info("Compute logical min offset: {}, topic: {}, queueId: {}",
                                this.getMinOffsetInQueue(), this.topic, this.queueId);
                        // This maybe not take effect, when not every consume queue has an extended file.
                        if (isExtAddr(tagsCode)) {
                            minExtAddr = tagsCode;
                        }
                        break;
                    }
                }
            } catch (Exception e) {
                log.error("Exception thrown when correctMinOffset", e);
            } finally {
                result.release();
            }
        }

        if (isExtReadEnable()) {
            this.consumeQueueExt.truncateByMinAddress(minExtAddr);
        }
    }

    @Override
    public long getMinOffsetInQueue() {
        return this.minLogicOffset / CQ_STORE_UNIT_SIZE;
    }

    /**
     * 逻辑消息的保存逻辑比较长，主要的逻辑步骤如下：
     * <p>
     * 检查对应的文件是不是可写的状态，以及写入的重试次数是否达到上限30次
     * 如果消息扩展服务开启了，则保存对应的扩展信息到扩展文件队列中
     * 组装消息进行写入
     * 如果写入成功，则更新CheckPoint文件中的逻辑日志落盘时间
     * <p>
     * <p>
     *  其中组装消息写入被抽出到另外一个方法putMessagePositionInfo中。主要逻辑如下：
     * <p>
     * 申请20个字节长度的buffer，然后依次拼接消息在CommitLog中的偏移量，消息长度和消息的tagCode
     * 然后获取 MappedFile，并把消息保存进去，同时更新maxPhysicOffset字段
     * ————————————————
     * 版权声明：本文为CSDN博主「szhlcy」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
     * 原文链接：https://blog.csdn.net/szhlcy/article/details/115146311
     *
     * @param request the request containing dispatch information.
     */
    @Override
    public void putMessagePositionInfoWrapper(DispatchRequest request) {
        //最大的重试次数
        final int maxRetries = 30;
        //检查对应的ConsumeQueue文件是不是可写
        boolean canWrite = this.messageStore.getRunningFlags().isCQWriteable();
        //可写 并且 重试次数还没达到30次，则进行写入
        for (int i = 0; i < maxRetries && canWrite; i++) {
            //获取消息的  Tag
            long tagsCode = request.getTagsCode();
            //消息扩展服务是否开启
            if (isExtWriteEnable()) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                cqExtUnit.setFilterBitMap(request.getBitMap());
                cqExtUnit.setMsgStoreTime(request.getStoreTimestamp());
                cqExtUnit.setTagsCode(request.getTagsCode());

                long extAddr = this.consumeQueueExt.put(cqExtUnit);
                if (isExtAddr(extAddr)) {
                    tagsCode = extAddr;
                } else {
                    log.warn("Save consume queue extend fail, So just save tagsCode! {}, topic:{}, queueId:{}, offset:{}", cqExtUnit,
                            topic, queueId, request.getCommitLogOffset());
                }
            }
            //组装消息存储位置信息=》CommitLog中的偏移量，消息的大小，和小的Tag的hash值
            boolean result = this.putMessagePositionInfo(request.getCommitLogOffset(),
                    request.getMsgSize(), tagsCode, request.getConsumeQueueOffset());
            //设置CheckPoint文件中的逻辑日志落盘时间
            if (result) {
                if (this.messageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE ||
                        this.messageStore.getMessageStoreConfig().isEnableDLegerCommitLog()) {
                    this.messageStore.getStoreCheckpoint().setPhysicMsgTimestamp(request.getStoreTimestamp());
                }
                this.messageStore.getStoreCheckpoint().setLogicsMsgTimestamp(request.getStoreTimestamp());
                if (checkMultiDispatchQueue(request)) {
                    multiDispatchLmqQueue(request, maxRetries);
                }
                return;
            } else {
                // XXX: warn and notify me
                log.warn("[BUG]put commit log position info to " + topic + ":" + queueId + " " + request.getCommitLogOffset()
                        + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }

        // XXX: warn and notify me
        log.error("[BUG]consume queue can not write, {} {}", this.topic, this.queueId);
        this.messageStore.getRunningFlags().makeLogicsQueueError();
    }

    private boolean checkMultiDispatchQueue(DispatchRequest dispatchRequest) {
        if (!this.messageStore.getMessageStoreConfig().isEnableMultiDispatch()
                || dispatchRequest.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
                || dispatchRequest.getTopic().equals(TimerMessageStore.TIMER_TOPIC)
                || dispatchRequest.getTopic().equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)) {
            return false;
        }
        Map<String, String> prop = dispatchRequest.getPropertiesMap();
        if (prop == null || prop.isEmpty()) {
            return false;
        }
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
            return false;
        }
        return true;
    }

    private void multiDispatchLmqQueue(DispatchRequest request, int maxRetries) {
        Map<String, String> prop = request.getPropertiesMap();
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        String[] queueOffsets = multiQueueOffset.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        if (queues.length != queueOffsets.length) {
            log.error("[bug] queues.length!=queueOffsets.length ", request.getTopic());
            return;
        }
        for (int i = 0; i < queues.length; i++) {
            String queueName = queues[i];
            if (StringUtils.contains(queueName, File.separator)) {
                continue;
            }
            long queueOffset = Long.parseLong(queueOffsets[i]);
            int queueId = request.getQueueId();
            if (this.messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
                queueId = 0;
            }
            doDispatchLmqQueue(request, maxRetries, queueName, queueOffset, queueId);

        }
        return;
    }

    private void doDispatchLmqQueue(DispatchRequest request, int maxRetries, String queueName, long queueOffset,
                                    int queueId) {
        ConsumeQueueInterface cq = this.messageStore.findConsumeQueue(queueName, queueId);
        boolean canWrite = this.messageStore.getRunningFlags().isCQWriteable();
        for (int i = 0; i < maxRetries && canWrite; i++) {
            boolean result = ((ConsumeQueue) cq).putMessagePositionInfo(request.getCommitLogOffset(), request.getMsgSize(),
                    request.getTagsCode(),
                    queueOffset);
            if (result) {
                break;
            } else {
                log.warn("[BUG]put commit log position info to " + queueName + ":" + queueId + " " + request.getCommitLogOffset()
                        + " failed, retry " + i + " times");

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.warn("", e);
                }
            }
        }
    }

    @Override
    public void assignQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg) {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        long queueOffset = queueOffsetOperator.getQueueOffset(topicQueueKey);
        msg.setQueueOffset(queueOffset);


        // Handling the multi dispatch message. In the context of a light message queue (as defined in RIP-28),
        // light message queues are constructed based on message properties, which requires special handling of queue offset of the light message queue.
        if (!isNeedHandleMultiDispatch(msg)) {
            return;
        }
        String multiDispatchQueue = msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return;
        }
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        Long[] queueOffsets = new Long[queues.length];
        for (int i = 0; i < queues.length; i++) {
            String key = queueKey(queues[i], msg);
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
                queueOffsets[i] = queueOffsetOperator.getLmqOffset(key);
            }
        }
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET,
                StringUtils.join(queueOffsets, MixAll.MULTI_DISPATCH_QUEUE_SPLITTER));
        removeWaitStorePropertyString(msg);
    }

    @Override
    public void increaseQueueOffset(QueueOffsetOperator queueOffsetOperator, MessageExtBrokerInner msg,
                                    short messageNum) {
        String topicQueueKey = getTopic() + "-" + getQueueId();
        queueOffsetOperator.increaseQueueOffset(topicQueueKey, messageNum);

        // Handling the multi dispatch message. In the context of a light message queue (as defined in RIP-28),
        // light message queues are constructed based on message properties, which requires special handling of queue offset of the light message queue.
        if (!isNeedHandleMultiDispatch(msg)) {
            return;
        }
        String multiDispatchQueue = msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        if (StringUtils.isBlank(multiDispatchQueue)) {
            return;
        }
        String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
        for (int i = 0; i < queues.length; i++) {
            String key = queueKey(queues[i], msg);
            if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(key)) {
                queueOffsetOperator.increaseLmqOffset(key, (short) 1);
            }
        }
    }

    public boolean isNeedHandleMultiDispatch(MessageExtBrokerInner msg) {
        return messageStore.getMessageStoreConfig().isEnableMultiDispatch()
                && !msg.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
                && !msg.getTopic().equals(TimerMessageStore.TIMER_TOPIC)
                && !msg.getTopic().equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC);
    }

    public String queueKey(String queueName, MessageExtBrokerInner msgInner) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(queueName);
        keyBuilder.append('-');
        int queueId = msgInner.getQueueId();
        if (messageStore.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
            queueId = 0;
        }
        keyBuilder.append(queueId);
        return keyBuilder.toString();
    }

    private void removeWaitStorePropertyString(MessageExtBrokerInner msgInner) {
        if (msgInner.getProperties().containsKey(MessageConst.PROPERTY_WAIT_STORE_MSG_OK)) {
            // There is no need to store "WAIT=true", remove it from propertiesString to save 9 bytes for each message.
            // It works for most case. In some cases msgInner.setPropertiesString invoked later and replace it.
            String waitStoreMsgOKValue = msgInner.getProperties().remove(MessageConst.PROPERTY_WAIT_STORE_MSG_OK);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            // Reput to properties, since msgInner.isWaitStoreMsgOK() will be invoked later
            msgInner.getProperties().put(MessageConst.PROPERTY_WAIT_STORE_MSG_OK, waitStoreMsgOKValue);
        } else {
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
        }
    }

    /**
     * @param offset   CommitLog文件的偏移量
     * @param size     消息的大小
     * @param tagsCode 消息的tag
     * @param cqOffset ConsumeQueue的偏移，这个偏移在添加消息到CommitLog的时候确定了
     * @return
     */
    private boolean putMessagePositionInfo(final long offset, final int size, final long tagsCode,
                                           final long cqOffset) {
        //如果CommitLog 的偏移量比consumequeue的最大偏移量还小，说明已经追加过了
        if (offset + size <= this.getMaxPhysicOffset()) {
            log.warn("Maybe try to build consume queue repeatedly maxPhysicOffset={} phyOffset={}", this.getMaxPhysicOffset(), offset);
            return true;
        }
        //把buffer重置
        this.byteBufferIndex.flip();
        //申请20个字节大小
        this.byteBufferIndex.limit(CQ_STORE_UNIT_SIZE);
        //设置在CommitLog中的偏移量
        this.byteBufferIndex.putLong(offset);
        //设置消息的大小
        this.byteBufferIndex.putInt(size);
        //设置消息的tag信息
        this.byteBufferIndex.putLong(tagsCode);
        //希望拼接到的偏移量=commitLog中的QUEUEOFFSET*20
        final long expectLogicOffset = cqOffset * CQ_STORE_UNIT_SIZE;

        //从映射文件队列中获取最后一个映射文件
        MappedFile mappedFile = this.mappedFileQueue.getLastMappedFile(expectLogicOffset);
        if (mappedFile != null) {

            //映射文是第一个创建、consumerOffset不是0，映射文件写位置是0
            if (mappedFile.isFirstCreateInQueue() && cqOffset != 0 && mappedFile.getWrotePosition() == 0) {
                //设置最小的逻辑偏移量 为 对应消息的起始偏移量
                this.minLogicOffset = expectLogicOffset;
                this.mappedFileQueue.setFlushedWhere(expectLogicOffset);
                this.mappedFileQueue.setCommittedWhere(expectLogicOffset);
                //填充文件=》如果不是第一次拼接，则指定位置进行拼接
                this.fillPreBlank(mappedFile, expectLogicOffset);
                log.info("fill pre blank space " + mappedFile.getFileName() + " " + expectLogicOffset + " "
                        + mappedFile.getWrotePosition());
            }
            //consumerOffset不是0，则表示不是文件中的第一个记录
            if (cqOffset != 0) {
                //计算当前的文件提交到的位置
                long currentLogicOffset = mappedFile.getWrotePosition() + mappedFile.getFileFromOffset();
                //如果文件写入的偏移量 已经 大于这个消息写入的初始偏移量， 表示这个消息重复了
                if (expectLogicOffset < currentLogicOffset) {
                    log.warn("Build  consume queue repeatedly, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                            expectLogicOffset, currentLogicOffset, this.topic, this.queueId, expectLogicOffset - currentLogicOffset);
                    return true;
                }
                //如果两个值不相等， 说明队列的顺序可能有问题
                if (expectLogicOffset != currentLogicOffset) {
                    LOG_ERROR.warn(
                            "[BUG]logic queue order maybe wrong, expectLogicOffset: {} currentLogicOffset: {} Topic: {} QID: {} Diff: {}",
                            expectLogicOffset,
                            currentLogicOffset,
                            this.topic,
                            this.queueId,
                            expectLogicOffset - currentLogicOffset
                    );
                }
            }
            //这只最大的物理偏移量为CommitLog 的偏移量
            this.setMaxPhysicOffset(offset + size);
            //消息写入映射文件
            return mappedFile.appendMessage(this.byteBufferIndex.array());
        }
        return false;
    }

    private void fillPreBlank(final MappedFile mappedFile, final long untilWhere) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(CQ_STORE_UNIT_SIZE);
        byteBuffer.putLong(0L);
        byteBuffer.putInt(Integer.MAX_VALUE);
        byteBuffer.putLong(0L);

        int until = (int) (untilWhere % this.mappedFileQueue.getMappedFileSize());
        for (int i = 0; i < until; i += CQ_STORE_UNIT_SIZE) {
            mappedFile.appendMessage(byteBuffer.array());
        }
    }

    /**
     * 根据消息的index获取消息单元
     *  逻辑比较简单，就是依据单个消息单元的大小为20字节，来计算在文件中的位置，然后去出来
     *
     * @param startIndex
     * @return
     */
    public SelectMappedBufferResult getIndexBuffer(final long startIndex) {
        int mappedFileSize = this.mappedFileSize;
        //计算消息的在文件中的便宜offset
        long offset = startIndex * CQ_STORE_UNIT_SIZE;
        //偏移量小于最小的逻辑偏移量，则说明消息在文件中
        if (offset >= this.getMinLogicOffset()) {
            //根据offset查询映射文件
            MappedFile mappedFile = this.mappedFileQueue.findMappedFileByOffset(offset);
            if (mappedFile != null) {
                return mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
            }
        }
        return null;
    }

    @Override
    public ReferredIterator<CqUnit> iterateFrom(long startOffset) {
        SelectMappedBufferResult sbr = getIndexBuffer(startOffset);
        if (sbr == null) {
            return null;
        }
        return new ConsumeQueueIterator(sbr);
    }

    @Override
    public CqUnit get(long offset) {
        ReferredIterator<CqUnit> it = iterateFrom(offset);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public CqUnit getEarliestUnit() {
        /**
         * here maybe should not return null
         */
        ReferredIterator<CqUnit> it = iterateFrom(minLogicOffset / CQ_STORE_UNIT_SIZE);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public CqUnit getLatestUnit() {
        ReferredIterator<CqUnit> it = iterateFrom((mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE) - 1);
        if (it == null) {
            return null;
        }
        return it.nextAndRelease();
    }

    @Override
    public boolean isFirstFileAvailable() {
        return false;
    }

    @Override
    public boolean isFirstFileExist() {
        return false;
    }

    private class ConsumeQueueIterator implements ReferredIterator<CqUnit> {
        private SelectMappedBufferResult sbr;
        private int relativePos = 0;

        public ConsumeQueueIterator(SelectMappedBufferResult sbr) {
            this.sbr = sbr;
            if (sbr != null && sbr.getByteBuffer() != null) {
                relativePos = sbr.getByteBuffer().position();
            }
        }

        @Override
        public boolean hasNext() {
            if (sbr == null || sbr.getByteBuffer() == null) {
                return false;
            }

            return sbr.getByteBuffer().hasRemaining();
        }

        @Override
        public CqUnit next() {
            if (!hasNext()) {
                return null;
            }
            long queueOffset = (sbr.getStartOffset() + sbr.getByteBuffer().position() - relativePos) / CQ_STORE_UNIT_SIZE;
            CqUnit cqUnit = new CqUnit(queueOffset,
                    sbr.getByteBuffer().getLong(),
                    sbr.getByteBuffer().getInt(),
                    sbr.getByteBuffer().getLong());

            if (isExtAddr(cqUnit.getTagsCode())) {
                ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                boolean extRet = getExt(cqUnit.getTagsCode(), cqExtUnit);
                if (extRet) {
                    cqUnit.setTagsCode(cqExtUnit.getTagsCode());
                    cqUnit.setCqExtUnit(cqExtUnit);
                } else {
                    // can't find ext content.Client will filter messages by tag also.
                    log.error("[BUG] can't find consume queue extend file content! addr={}, offsetPy={}, sizePy={}, topic={}",
                            cqUnit.getTagsCode(), cqUnit.getPos(), cqUnit.getPos(), getTopic());
                }
            }
            return cqUnit;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove");
        }

        @Override
        public void release() {
            if (sbr != null) {
                sbr.release();
                sbr = null;
            }
        }

        @Override
        public CqUnit nextAndRelease() {
            try {
                return next();
            } finally {
                release();
            }
        }
    }

    public ConsumeQueueExt.CqExtUnit getExt(final long offset) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset);
        }
        return null;
    }

    public boolean getExt(final long offset, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        if (isExtReadEnable()) {
            return this.consumeQueueExt.get(offset, cqExtUnit);
        }
        return false;
    }

    @Override
    public long getMinLogicOffset() {
        return minLogicOffset;
    }

    public void setMinLogicOffset(long minLogicOffset) {
        this.minLogicOffset = minLogicOffset;
    }

    @Override
    public long rollNextFile(final long nextBeginOffset) {
        int mappedFileSize = this.mappedFileSize;
        int totalUnitsInFile = mappedFileSize / CQ_STORE_UNIT_SIZE;
        return nextBeginOffset + totalUnitsInFile - nextBeginOffset % totalUnitsInFile;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public int getQueueId() {
        return queueId;
    }

    @Override
    public CQType getCQType() {
        return CQType.SimpleCQ;
    }

    @Override
    public long getMaxPhysicOffset() {
        return maxPhysicOffset;
    }

    public void setMaxPhysicOffset(long maxPhysicOffset) {
        this.maxPhysicOffset = maxPhysicOffset;
    }

    @Override
    public void destroy() {
        this.setMaxPhysicOffset(-1);
        this.minLogicOffset = 0;
        this.mappedFileQueue.destroy();
        if (isExtReadEnable()) {
            this.consumeQueueExt.destroy();
        }
    }

    @Override
    public long getMessageTotalInQueue() {
        return this.getMaxOffsetInQueue() - this.getMinOffsetInQueue();
    }

    @Override
    public long getMaxOffsetInQueue() {
        return this.mappedFileQueue.getMaxOffset() / CQ_STORE_UNIT_SIZE;
    }

    @Override
    public void checkSelf() {
        mappedFileQueue.checkSelf();
        if (isExtReadEnable()) {
            this.consumeQueueExt.checkSelf();
        }
    }

    protected boolean isExtReadEnable() {
        return this.consumeQueueExt != null;
    }

    protected boolean isExtWriteEnable() {
        return this.consumeQueueExt != null
                && this.messageStore.getMessageStoreConfig().isEnableConsumeQueueExt();
    }

    /**
     * Check {@code tagsCode} is address of extend file or tags code.
     */
    public boolean isExtAddr(long tagsCode) {
        return ConsumeQueueExt.isExtAddr(tagsCode);
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {
        mappedFileQueue.swapMap(reserveNum, forceSwapIntervalMs, normalSwapIntervalMs);
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {
        mappedFileQueue.cleanSwappedMap(forceCleanSwapIntervalMs);
    }

    @Override
    public long estimateMessageCount(long from, long to, MessageFilter filter) {
        long physicalOffsetFrom = from * CQ_STORE_UNIT_SIZE;
        long physicalOffsetTo = to * CQ_STORE_UNIT_SIZE;
        List<MappedFile> mappedFiles = mappedFileQueue.range(physicalOffsetFrom, physicalOffsetTo);
        if (mappedFiles.isEmpty()) {
            return -1;
        }

        boolean sample = false;
        long match = 0;
        long raw = 0;

        for (MappedFile mappedFile : mappedFiles) {
            int start = 0;
            int len = mappedFile.getFileSize();

            // calculate start and len for first segment and last segment to reduce scanning
            // first file segment
            if (mappedFile.getFileFromOffset() <= physicalOffsetFrom) {
                start = (int) (physicalOffsetFrom - mappedFile.getFileFromOffset());
                if (mappedFile.getFileFromOffset() + mappedFile.getFileSize() >= physicalOffsetTo) {
                    // current mapped file covers search range completely.
                    len = (int) (physicalOffsetTo - physicalOffsetFrom);
                } else {
                    len = mappedFile.getFileSize() - start;
                }
            }

            // last file segment
            if (0 == start && mappedFile.getFileFromOffset() + mappedFile.getFileSize() > physicalOffsetTo) {
                len = (int) (physicalOffsetTo - mappedFile.getFileFromOffset());
            }

            // select partial data to scan
            SelectMappedBufferResult slice = mappedFile.selectMappedBuffer(start, len);
            if (null != slice) {
                try {
                    ByteBuffer buffer = slice.getByteBuffer();
                    int current = 0;
                    while (current < len) {
                        // skip physicalOffset and message length fields.
                        buffer.position(current + MSG_TAG_OFFSET_INDEX);
                        long tagCode = buffer.getLong();
                        ConsumeQueueExt.CqExtUnit ext = null;
                        if (isExtWriteEnable()) {
                            ext = consumeQueueExt.get(tagCode);
                            tagCode = ext.getTagsCode();
                        }
                        if (filter.isMatchedByConsumeQueue(tagCode, ext)) {
                            match++;
                        }
                        raw++;
                        current += CQ_STORE_UNIT_SIZE;

                        if (raw >= messageStore.getMessageStoreConfig().getMaxConsumeQueueScan()) {
                            sample = true;
                            break;
                        }

                        if (match > messageStore.getMessageStoreConfig().getSampleCountThreshold()) {
                            sample = true;
                            break;
                        }
                    }
                } finally {
                    slice.release();
                }
            }
            // we have scanned enough entries, now is the time to return an educated guess.
            if (sample) {
                break;
            }
        }

        long result = match;
        if (sample) {
            if (0 == raw) {
                log.error("[BUG]. Raw should NOT be 0");
                return 0;
            }
            result = (long) (match * (to - from) * 1.0 / raw);
        }
        log.debug("Result={}, raw={}, match={}, sample={}", result, raw, match, sample);
        return result;
    }
}
