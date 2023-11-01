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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.List;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;

/**
 * Index文件由IndexService类提供服务（大文件）
 * <p>
 *  IndexFile（索引文件）提供了一种可以通过key或时间区间来查询消息的方法。Index文件的存储位置是：HOME\store\index{fileName}，文件名fileName是以创建时的时间戳命名的，固定的单个IndexFile文件大小约为400M，
 * 一个IndexFile可以保存 2000W个索引，IndexFile的底层存储设计为在文件系统中实现HashMap结构，故rocketmq的索引文件其底层实现为hash索引。
 */
public class IndexFile {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    //hash曹的大小
    private static int hashSlotSize = 4;
    /**
     * Each index's store unit. Format:
     * <pre>
     * ┌───────────────┬───────────────────────────────┬───────────────┬───────────────┐
     * │ Key HashCode  │        Physical Offset        │   Time Diff   │ Next Index Pos│
     * │   (4 Bytes)   │          (8 Bytes)            │   (4 Bytes)   │   (4 Bytes)   │
     * ├───────────────┴───────────────────────────────┴───────────────┴───────────────┤
     * │                                 Index Store Unit                              │
     * │                                                                               │
     * </pre>
     * Each index's store unit. Size:
     * Key HashCode(4) + Physical Offset(8) + Time Diff(4) + Next Index Pos(4) = 20 Bytes
     */
    //一个index结构的大小
    private static int indexSize = 20;
    //无效的index
    private static int invalidIndex = 0;
    //hash槽总数
    private final int hashSlotNum;
    //index的数量
    private final int indexNum;
    //IndexFile文件的映射文件对象
    private final int fileTotalSize;
    private final MappedFile mappedFile;
    private final MappedByteBuffer mappedByteBuffer;
    //IndexFile的头信息
    private final IndexHeader indexHeader;

    /**
     *  构造方法主要就是对应的主要参数的设置，根据入参计算整个文件的大小（IndexFile的头大小 + hash槽的大小 x hash槽的数量 + index结构的大小 x index结构的数量），然后创建文件，设置文件头对象IndexHead
     *
     * @param fileName
     * @param hashSlotNum
     * @param indexNum
     * @param endPhyOffset
     * @param endTimestamp
     * @throws IOException
     */
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
                     final long endPhyOffset, final long endTimestamp) throws IOException {
        //计算文件的大小 = IndexFile的头大小 + hash槽的大小*hash槽的数量 + index结构的大小*index结构的数量
        this.fileTotalSize =
                IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        //获取映射文件对象
        this.mappedFile = new DefaultMappedFile(fileName, fileTotalSize);
        //获取对应文件的缓存
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        //设置hash槽数量
        this.hashSlotNum = hashSlotNum;
        //设置index结构的数量
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        //创建文件对应的IndexHead对象
        this.indexHeader = new IndexHeader(byteBuffer);

        //初始化头文件的beginPhyOffset 和 endPhyOffset
        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }
        //初始化头文件的beginTimestamp 和 endTimestamp
        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public int getFileSize() {
        return this.fileTotalSize;
    }

    public void load() {
        this.indexHeader.load();
    }

    public void shutdown() {
        this.flush();
        UtilAll.cleanBuffer(this.mappedByteBuffer);
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        if (this.mappedFile.hold()) {
            this.indexHeader.updateByteBuffer();
            this.mappedByteBuffer.force();
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * 检查IndexHead类中记录的indexCount值和IndexFile类中记录的indexNum进行比较，检查文件是否已经满了，如果满了直接返回
     * 计算传入key对应的hash槽的位置，并检查要插入的槽位是否已经存在值了，如果已经存在值了，检查是不是无效值，如果不是则需要记录。在插入index信息的时候保存
     * 吧当前key的索引值，插入对应的hash槽中
     * 计算对应的index链表的位置，然后插入index信息，如果之前hash槽分配存在hash冲突，则在把冲突的上一个key的index的值，保存在slotValue中
     * ————————————————
     * 版权声明：本文为CSDN博主「szhlcy」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
     * 原文链接：https://blog.csdn.net/szhlcy/article/details/115907118
     *
     * @param key
     * @param phyOffset
     * @param storeTimestamp
     * @return
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        //如果已经构建的索引index数量 < 最大的index数量，则进行插入，否则直接返回 false
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            //计算key 的 hash值，使用的是String自带的hashcode方法计算
            int keyHash = indexKeyHashMethod(key);
            // 计算key对应的hash槽的位置
            int slotPos = keyHash % this.hashSlotNum;
            //计算对应槽为的偏移量   IndexFile的头长度+hash槽的位置*hash槽大小  40+位置*4
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                //从对应的槽位的位置开始 获取4个字节的长度 得到对应topic的key对应索引的位置
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                //检查对应槽位的值 是不是无效的索引，如果不是说明这次插入的key跟之前的key冲突，则要取出之前的keu
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // 存储时间 - 头文件记录的开始时间得到 时间差
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
                //转换时间
                timeDiff = timeDiff / 1000;
                //如果头文件记录的开始时间小于0，则时间差记为0 ， 如果大于int最大值，则为最大值，如果时间差小于0，也记录为0
                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }
                /**
                 * 计算 需要设置值的index偏移量  IndexFile头大小+hash槽数量*hash槽大小+IndexFile的indexCount*index大小
                 * 也就是 40+500w*4+20*indexCount
                 */
                int absIndexPos =
                        IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + this.indexHeader.getIndexCount() * indexSize;

                //设置  index中的 keyHash
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                //设置 index中的 phyOffset
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                //设置 index中的 timeDiff
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                //设置 index中的 slotValue
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                //设置 在hash槽中的 index
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());
                //如果indexCount 小于1，则表示是第一个存入的消息信息 则设置对应的初始信息
                if (this.indexHeader.getIndexCount() <= 1) {
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                //如果对应的 key的索引是无效索引
                if (invalidIndex == slotValue) {
                    this.indexHeader.incHashSlotCount();
                }
                //增加indexCount值
                this.indexHeader.incIndexCount();
                //设置对应的最后一个消息的偏移量
                this.indexHeader.setEndPhyOffset(phyOffset);
                //设置对应的最后一个消息的存储时间
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                    + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0) {
            keyHashPositive = 0;
        }
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp();
        result = result || end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp();
        return result;
    }

    /**
     * 根据偏移量和落盘时间段获取消息的物理偏移量集合
     * 根据时间区间查询和key来进行查询消息的selectPhyOffset方法
     * <p>
     * 根据传入的key，计算hash槽的位置
     * 获取hash槽记录的index链表的位置的值
     * 获取index链表中的slotValue值是否大于0，大于0表示存在hash冲突，也就是存在key相同的消息，需要进入步骤4进一步寻找，否则直接返回
     * 根据slotValue记录的值，寻找对应的index链表的index信息。同时校验，index记录的timeOffset和IndexHead记录的beginTimestamp的和是否在传入的时间区间内。在则继续获取slotValue重复步骤4，直到找到不符合的消息。
     * ————————————————
     * 版权声明：本文为CSDN博主「szhlcy」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
     * 原文链接：https://blog.csdn.net/szhlcy/article/details/115907118
     *
     * @param phyOffsets 封装逻辑偏移量值的集合
     * @param key        开始寻找的key  结构为消息的topic#key
     * @param maxNum     寻找的数量
     * @param begin      落盘时间段开始时间
     * @param end        落盘时间段结束时间
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
                                final long begin, final long end) {
        if (this.mappedFile.hold()) {
            //计算key的hash槽的位置
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            //计算槽位的偏移量信息
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            try {
                //获取key对应的索引信息

                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                //如果是无效索引则不处理，意思就是没有hash冲突的情况下则不进一步处理，否则需要获取之前的冲突的key

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                        || this.indexHeader.getIndexCount() <= 1) {
                } else {
                    //迭代获取冲突的消息直到没有冲突

                    for (int nextIndexToRead = slotValue; ; ) {
                        //获取完毕，就结束

                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }
                        //计算index结构的偏移量
                        int absIndexPos =
                                IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                        + nextIndexToRead * indexSize;
                        //获取key的hash值
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        //获取消息的物理偏移量
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);

                        //获取时间位移
                        long timeDiff = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        //获取槽位冲突的上一个key的index信息
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        //如果时间偏移小于0，则进行处理
                        if (timeDiff < 0) {
                            break;
                        }

                        timeDiff *= 1000L;
                        //计算消息的存储时间
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        //检查消息是否符合
                        boolean timeMatched = timeRead >= begin && timeRead <= end;
                        //符合条件的消息的 物理偏移量 添加到结果集中
                        if (keyHash == keyHashRead && timeMatched) {
                            phyOffsets.add(phyOffsetRead);
                        }
                        //如果槽位冲突的上一个key的index信息不合法，则直接跳过，否则处理冲突的key
                        if (prevIndexRead <= invalidIndex
                                || prevIndexRead > this.indexHeader.getIndexCount()
                                || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                this.mappedFile.release();
            }
        }
    }
}
