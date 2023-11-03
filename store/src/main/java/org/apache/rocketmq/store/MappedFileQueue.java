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

import com.google.common.collect.Lists;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.logfile.DefaultMappedFile;
import org.apache.rocketmq.store.logfile.MappedFile;

/**
 *  MappedFileQueue这个类的属性相对来说比较少，其中需要说的是，AllocateMappedFileService类型的字段，这个对象的作用是根据情况来决定是否需要提前创建好MappedFile对象供后续的直接使用。
 * 而这个参数是在构造MappedFileQueue对象的时候的一个参数。只有在CommitLog中构造时才会传入AllocateMappedFileService，在ConsumeQueue并没有传入。
 * ————————————————
 * 版权声明：本文为CSDN博主「szhlcy」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
 * 原文链接：https://blog.csdn.net/szhlcy/article/details/114541473
 */
public class MappedFileQueue implements Swappable {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final Logger LOG_ERROR = LoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    //文件的存储路径
    protected final String storePath;
    //映射文件大小，指的是单个文件的大小，比如CommitLog大小为1G
    protected final int mappedFileSize;
    //并发线程安全队列存储映射文件
    protected final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();

    protected final AllocateMappedFileService allocateMappedFileService;
    //刷新完的位置
    protected long flushedWhere = 0;
    //提交完成的位置
    protected long committedWhere = 0;
    //存储时间
    protected volatile long storeTimestamp = 0;

    /**
     *  MappedFileQueue只有一个全参构造器，分别是传入文件的存储路径storePath，单个存储文件的大小mappedFileSize和提前创建MappedFile对象的allocateMappedFileService
     *
     * @param storePath
     * @param mappedFileSize
     * @param allocateMappedFileService
     */
    public MappedFileQueue(final String storePath, int mappedFileSize,
                           AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    /**
     * 检查文件的是否完整，检查的方式。上一个文件的起始偏移量减去当前文件的起始偏移量，如果差值=mappedFileSize那么说明文件是完整的，否则有损坏
     * <p>
     * 这里检查文件是否被破坏的原理，就是检查文件的大小是不是等于前一个文件的起始偏移量和后一个文件的起始偏移量是不是等于文件大小。
     * 而这里的起始偏移量又是在MappedFile进行获取的fileFromOffset，而这个值就是我们在构造MappedFile的时候传入的文件名转化得到的
     */
    public void checkSelf() {
        List<MappedFile> mappedFiles = new ArrayList<>(this.mappedFiles);
        //检查文件组是否为空
        if (!mappedFiles.isEmpty()) {
            //对文件进行迭代，一个一个进行检查
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    //用当前文件的真实偏移量-上一个文件的其实偏移量 正常情况下应该等于一个文件的大小。如果不相等，说明文件存在问题
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                                pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    public MappedFile getConsumeQueueMappedFileByTime(final long timestamp, CommitLog commitLog,
                                                      BoundaryType boundaryType) {
        Object[] mfs = copyMappedFiles(0);
        if (null == mfs) {
            return null;
        }

        /*
         * Make sure each mapped file in consume queue has accurate start and stop time in accordance with commit log
         * mapped files. Note last modified time from file system is not reliable.
         */
        for (int i = mfs.length - 1; i >= 0; i--) {
            DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];
            // Figure out the earliest message store time in the consume queue mapped file.
            if (mappedFile.getStartTimestamp() < 0) {
                SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0, ConsumeQueue.CQ_STORE_UNIT_SIZE);
                if (null != selectMappedBufferResult) {
                    try {
                        ByteBuffer buffer = selectMappedBufferResult.getByteBuffer();
                        long physicalOffset = buffer.getLong();
                        int messageSize = buffer.getInt();
                        long messageStoreTime = commitLog.pickupStoreTimestamp(physicalOffset, messageSize);
                        if (messageStoreTime > 0) {
                            mappedFile.setStartTimestamp(messageStoreTime);
                        }
                    } finally {
                        selectMappedBufferResult.release();
                    }
                }
            }
            // Figure out the latest message store time in the consume queue mapped file.
            if (i < mfs.length - 1 && mappedFile.getStopTimestamp() < 0) {
                SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(mappedFileSize - ConsumeQueue.CQ_STORE_UNIT_SIZE, ConsumeQueue.CQ_STORE_UNIT_SIZE);
                if (null != selectMappedBufferResult) {
                    try {
                        ByteBuffer buffer = selectMappedBufferResult.getByteBuffer();
                        long physicalOffset = buffer.getLong();
                        int messageSize = buffer.getInt();
                        long messageStoreTime = commitLog.pickupStoreTimestamp(physicalOffset, messageSize);
                        if (messageStoreTime > 0) {
                            mappedFile.setStopTimestamp(messageStoreTime);
                        }
                    } finally {
                        selectMappedBufferResult.release();
                    }
                }
            }
        }

        switch (boundaryType) {
            case LOWER: {
                for (int i = 0; i < mfs.length; i++) {
                    DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];
                    if (i < mfs.length - 1) {
                        long stopTimestamp = mappedFile.getStopTimestamp();
                        if (stopTimestamp >= timestamp) {
                            return mappedFile;
                        }
                    }

                    // Just return the latest one.
                    if (i == mfs.length - 1) {
                        return mappedFile;
                    }
                }
            }
            case UPPER: {
                for (int i = mfs.length - 1; i >= 0; i--) {
                    DefaultMappedFile mappedFile = (DefaultMappedFile) mfs[i];
                    if (mappedFile.getStartTimestamp() <= timestamp) {
                        return mappedFile;
                    }
                }
            }

            default: {
                log.warn("Unknown boundary type");
                break;
            }
        }
        return null;
    }

    /**
     * 根据时间戳获取文件getMappedFileByTime
     *
     * @param timestamp
     * @return
     */
    public MappedFile getMappedFileByTime(final long timestamp) {
        //获取所有的文件映射对象MappedFile
        Object[] mfs = this.copyMappedFiles(0);
        //为null说明 mappedFiles 中没有MappedFile
        if (null == mfs)
            return null;

        for (int i = 0; i < mfs.length; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            //如果文件的最后修改时间大于等于参数时间，说文件在当前传入的时间之后进行修改了，就是需要寻找的文件
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }
        //如果没有找到合适的MappedFile 就用最后一个
        return (MappedFile) mfs[mfs.length - 1];
    }

    protected Object[] copyMappedFiles(final int reservedMappedFiles) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    /**
     * 根据偏移量截断文件
     *
     * @param offset
     */
    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<>();
        /**
         * 如果   文件的起始偏移量>指定截断偏移量offset  那么整个文件需要删除
         * 如果   文件的起始偏移量<指定截断偏移量offset<文件的最大偏移量  那么文件中的部分记录需要清除
         * 如果   文件的最大偏移量<指定截断偏移量offset  那么这个文件不需要进行处理
         */
        for (MappedFile file : this.mappedFiles) {
            //文件的开始偏移量+文件大小= 文件尾offset
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            //当前文件的最大偏移量大于 指定截断位置的偏移量，说明需要截断的位置就是在这个文件中
            if (fileTailOffset > offset) {
                //如果文件初始偏移量小于指定的偏移量，说明只需要截断文件中的一部分
                if (offset >= file.getFileFromOffset()) {
                    //设置映射文件写的位置
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    //设置文件commit的位置
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    //设置文件刷新的位置
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    //如果文件的起始偏移量也比指定的偏移量大，则说明这个文件整个需要丢弃
                    file.destroy(1000);
                    //需要删除的文件加上这个文件
                    willRemoveFiles.add(file);
                }
            }
        }
        //删除映射的文件
        this.deleteExpiredFile(willRemoveFiles);
    }

    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    /**
     * 加载文件
     * <p>
     *  这里的逻辑比较简单，就是根据传入的文件路径，加载对应的文件夹下面的文件，并创建文件映射，并加入到文件映射列表中去。这个方法在RocketMQ启动的时候回调用，用来加载系统中已经存在的消息日志文件。
     *
     * @return
     */
    public boolean load() {
        //根据传入的文件保存路径storePath 来获取文件
        File dir = new File(this.storePath);
        File[] ls = dir.listFiles();
        //文件列表不为空则进行加载
        if (ls != null) {
            return doLoad(Arrays.asList(ls));
        }
        return true;
    }

    public boolean doLoad(List<File> files) {
        //根据名称进行排序
        // ascending order
        files.sort(Comparator.comparing(File::getName));

        for (int i = 0; i < files.size(); i++) {
            File file = files.get(i);
            if (file.isDirectory()) {
                continue;
            }

            if (file.length() == 0 && i == files.size() - 1) {
                boolean ok = file.delete();
                log.warn("{} size is 0, auto delete. is_ok: {}", file, ok);
                continue;
            }
            //队列映射文件的大小不等于设置的文件类型的大小，说明加载到了最后的一个文件  比如 如果是commitLog那么对于的大小应该为1G
            if (file.length() != this.mappedFileSize) {
                log.warn(file + "\t" + file.length()
                        + " length not matched message store config value, please check it manually");
                return false;
            }

            try {
                //根据文件的路径和文件大小，创建对应的文件映射，然后加入到映射列表中
                MappedFile mappedFile = new DefaultMappedFile(file.getPath(), mappedFileSize);

                mappedFile.setWrotePosition(this.mappedFileSize);
                mappedFile.setFlushedPosition(this.mappedFileSize);
                mappedFile.setCommittedPosition(this.mappedFileSize);
                this.mappedFiles.add(mappedFile);
                log.info("load " + file.getPath() + " OK");
            } catch (IOException e) {
                log.error("load file " + file + " error", e);
                return false;
            }
        }
        return true;
    }

    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty())
            return 0;

        long committed = this.getFlushedWhere();
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * 获取最后一个文件
     *
     * @param startOffset
     * @param needCreate
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset, boolean needCreate) {
        long createOffset = -1;
        MappedFile mappedFileLast = getLastMappedFile();

        //如果为null,
        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        //
        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        if (createOffset != -1 && needCreate) {
            return tryCreateMappedFile(createOffset);
        }

        return mappedFileLast;
    }

    public boolean isMappedFilesEmpty() {
        return this.mappedFiles.isEmpty();
    }

    public boolean isEmptyOrCurrentFileFull() {
        MappedFile mappedFileLast = getLastMappedFile();
        if (mappedFileLast == null) {
            return true;
        }
        if (mappedFileLast.isFull()) {
            return true;
        }
        return false;
    }

    public boolean shouldRoll(final int msgSize) {
        if (isEmptyOrCurrentFileFull()) {
            return true;
        }
        MappedFile mappedFileLast = getLastMappedFile();
        if (mappedFileLast.getWrotePosition() + msgSize > mappedFileLast.getFileSize()) {
            return true;
        }
        return false;
    }

    /**
     * 尝试创建MappedFile
     *
     * @param createOffset
     * @return
     */
    public MappedFile tryCreateMappedFile(long createOffset) {
        String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
        String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset
                + this.mappedFileSize);
        return doCreateMappedFile(nextFilePath, nextNextFilePath);
    }

    /**
     * 尝试创建MappedFile
     *
     * @param nextFilePath
     * @param nextNextFilePath
     * @return
     */
    protected MappedFile doCreateMappedFile(String nextFilePath, String nextNextFilePath) {
        MappedFile mappedFile = null;

        //如果异步服务对象不为空，那么就采用异步创建文件的方式
        if (this.allocateMappedFileService != null) {
            mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath,
                    nextNextFilePath, this.mappedFileSize);
        } else {
            //否则就同步创建
            try {
                mappedFile = new DefaultMappedFile(nextFilePath, this.mappedFileSize);
            } catch (IOException e) {
                log.error("create mappedFile exception", e);
            }
        }

        if (mappedFile != null) {
            if (this.mappedFiles.isEmpty()) {
                mappedFile.setFirstCreateInQueue(true);
            }
            this.mappedFiles.add(mappedFile);
        }

        return mappedFile;
    }

    /**
     * 获取最后一个文件
     *
     * @param startOffset
     * @return
     */
    public MappedFile getLastMappedFile(final long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 获取最后一个文件
     * <p>
     * 这个方法的作用基本就是获取最后一个映射文件，然后进行消息的插入，或者获取最大消息偏移量等信息。
     *
     * @return
     */
    public MappedFile getLastMappedFile() {
        MappedFile[] mappedFiles = this.mappedFiles.toArray(new MappedFile[0]);
        //如果文件队列不为空则获取最后一个文件
        return mappedFiles.length == 0 ? null : mappedFiles[mappedFiles.length - 1];
    }

    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() +
                    mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff)
                return false;
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator(mappedFiles.size());

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - getCommittedWhere();
    }

    public long remainHowManyDataToFlush() {
        return getMaxOffset() - this.getFlushedWhere();
    }

    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    /**
     * 根据时间删除过期文件
     * 这个方法被用在定期删除过去的CommitLog文件，来保证内存空间
     *
     * @param expiredTime
     * @param deleteFilesInterval
     * @param intervalForcibly
     * @param cleanImmediately
     * @param deleteFileBatchMax
     * @return
     */
    public int deleteExpiredFileByTime(final long expiredTime,
                                       final int deleteFilesInterval,
                                       final long intervalForcibly,
                                       final boolean cleanImmediately,
                                       final int deleteFileBatchMax) {
        //获取映射文件列表
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs)
            return 0;

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> files = new ArrayList<>();
        int skipFileNum = 0;
        //如果映射文件列表为空直接返回
        if (null != mfs) {
            //do check before deleting
            checkSelf();
            //对映射文件进行遍历
            for (int i = 0; i < mfsLength; i++) {
                MappedFile mappedFile = (MappedFile) mfs[i];
                //文件最后的修改时间+过期时间= 文件最终能够存活的时间
                long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
                //如果当前时间大于文件能够存活的最大时间，比如 当前是2021-03-18 12：00：00 ，而文件最大存活时间2021-03-18 11：00：00 就需要删除。或者调用方法的时候指定了马上删除
                if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                    if (skipFileNum > 0) {
                        log.info("Delete CommitLog {} but skip {} files", mappedFile.getFileName(), skipFileNum);
                    }
                    //删除文件，就是解除对文件的引用
                    if (mappedFile.destroy(intervalForcibly)) {
                        //要删除的的文件加入到要删除的集合中
                        files.add(mappedFile);
                        //增加计数
                        deleteCount++;
                        //一次性最多删除的人为10
                        if (files.size() >= deleteFileBatchMax) {
                            break;
                        }
                        //如果删除时间间隔大于0，并且没有循环玩，则睡眠指定的删除间隔时长后在杀出
                        if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                            try {
                                Thread.sleep(deleteFilesInterval);
                            } catch (InterruptedException e) {
                            }
                        }
                    } else {
                        break;
                    }
                } else {
                    skipFileNum++;
                    //avoid deleting files in the middle
                    break;
                }
            }
        }

        //从文件映射队列中删除对应的文件映射
        deleteExpiredFile(files);
        //返回删除的文件个数

        return deleteCount;
    }

    /**
     * 根据偏移量删除文件deleteExpiredFileByOffset
     * <p>
     *  按照偏移量删除文件用于删除过期的ConsumeQueue文件，因为ConsumeQueue文件中信息的记录是定长的20byte，如果偏移量小于指定的偏移量表示都是之前的消息，可以直接删除。
     *
     * @param offset
     * @param unitSize
     * @return
     */
    public int deleteExpiredFileByOffset(long offset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                //unitSize是一个文件格式占用的长度 比如ConsumeQueue中一条记录长度为20byte  这里是获取一个文件中最后一条记录的起始偏移量，
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    //获取文件中最后一条记录的偏移量
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    //如果最大偏移量 < 指定的偏移量，则需要删除
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                                + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }
                //删除文件
                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        //  删除映射文件队列中的映射文件=》
        deleteExpiredFile(files);

        return deleteCount;
    }

    public int deleteExpiredFileByOffsetForTimerLog(long offset, int checkOffset, int unitSize) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy = false;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(checkOffset);
                try {
                    if (result != null) {
                        int position = result.getByteBuffer().position();
                        int size = result.getByteBuffer().getInt();//size
                        result.getByteBuffer().getLong(); //prev pos
                        int magic = result.getByteBuffer().getInt();
                        if (size == unitSize && (magic | 0xF) == 0xF) {
                            result.getByteBuffer().position(position + MixAll.UNIT_PRE_SIZE_FOR_MSG);
                            long maxOffsetPy = result.getByteBuffer().getLong();
                            destroy = maxOffsetPy < offset;
                            if (destroy) {
                                log.info("physic min commitlog offset " + offset + ", current mappedFile's max offset "
                                        + maxOffsetPy + ", delete it");
                            }
                        } else {
                            log.warn("Found error data in [{}] checkOffset:{} unitSize:{}", mappedFile.getFileName(),
                                    checkOffset, unitSize);
                        }
                    } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                        log.warn("Found a hanged consume queue file, attempting to delete it.");
                        destroy = true;
                    } else {
                        log.warn("this being not executed forever.");
                        break;
                    }
                } finally {
                    if (null != result) {
                        result.release();
                    }
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    public boolean flush(final int flushLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.getFlushedWhere(), this.getFlushedWhere() == 0);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.getFlushedWhere();
            this.setFlushedWhere(where);
            if (0 == flushLeastPages) {
                this.setStoreTimestamp(tmpTimeStamp);
            }
        }

        return result;
    }

    public synchronized boolean commit(final int commitLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.getCommittedWhere(), this.getCommittedWhere() == 0);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.getCommittedWhere();
            this.setCommittedWhere(where);
        }

        return result;
    }

    /**
     * 根据偏移量获取文件
     * Finds a mapped file by offset.
     *
     * @param offset                Offset.
     * @param returnFirstOnNotFound If the mapped file is not found, then return the first one.
     * @return Mapped file or null (when not found and returnFirstOnNotFound is <code>false</code>).
     */
    public MappedFile findMappedFileByOffset(final long offset, final boolean returnFirstOnNotFound) {
        try {
            //获取队列中第一个映射文件
            MappedFile firstMappedFile = this.getFirstMappedFile();
            //获取队列中最后一个映射文件
            MappedFile lastMappedFile = this.getLastMappedFile();
            //如果不存在文件则直接返回null
            if (firstMappedFile != null && lastMappedFile != null) {
                //如果要查找的偏移量offset不在所有的文件偏移量范围内，则打印错误日志
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                            offset,
                            firstMappedFile.getFileFromOffset(),
                            lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                            this.mappedFileSize,
                            this.mappedFiles.size());
                } else {
                    //(指定Offset-第一个文件的其实偏移量)/文件大小=第几个文件夹
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        //获取指定的映射文件
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }
                    //offset在指定的映射文件中，则直接返回对应的映射文件
                    if (targetFile != null && offset >= targetFile.getFileFromOffset()
                            && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }
                    //如果按索引在队列中找不到映射文件就遍历队列查找映射文件
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset()
                                && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                }
                //如果指定了没有找到文件就返回第一个映射文件，则直接返回第一个映射文件
                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    public MappedFile findMappedFileByOffset(final long offset) {
        return findMappedFileByOffset(offset, false);
    }

    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    public boolean retryDeleteFirstFile(final long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    List<MappedFile> tmpFiles = new ArrayList<>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    public void shutdown(final long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.setFlushedWhere(0);

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }

    @Override
    public void swapMap(int reserveNum, long forceSwapIntervalMs, long normalSwapIntervalMs) {

        if (mappedFiles.isEmpty()) {
            return;
        }

        if (reserveNum < 3) {
            reserveNum = 3;
        }

        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs) {
            return;
        }

        for (int i = mfs.length - reserveNum - 1; i >= 0; i--) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > forceSwapIntervalMs) {
                mappedFile.swapMap();
                continue;
            }
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > normalSwapIntervalMs
                    && mappedFile.getMappedByteBufferAccessCountSinceLastSwap() > 0) {
                mappedFile.swapMap();
                continue;
            }
        }
    }

    @Override
    public void cleanSwappedMap(long forceCleanSwapIntervalMs) {

        if (mappedFiles.isEmpty()) {
            return;
        }

        int reserveNum = 3;
        Object[] mfs = this.copyMappedFiles(0);
        if (null == mfs) {
            return;
        }

        for (int i = mfs.length - reserveNum - 1; i >= 0; i--) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            if (System.currentTimeMillis() - mappedFile.getRecentSwapMapTime() > forceCleanSwapIntervalMs) {
                mappedFile.cleanSwapedMap(false);
            }
        }
    }

    public Object[] snapshot() {
        // return a safe copy
        return this.mappedFiles.toArray();
    }

    public Stream<MappedFile> stream() {
        return this.mappedFiles.stream();
    }

    public Stream<MappedFile> reversedStream() {
        return Lists.reverse(this.mappedFiles).stream();
    }

    public long getFlushedWhere() {
        return flushedWhere;
    }

    public void setFlushedWhere(long flushedWhere) {
        this.flushedWhere = flushedWhere;
    }

    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    public void setStoreTimestamp(long storeTimestamp) {
        this.storeTimestamp = storeTimestamp;
    }

    public List<MappedFile> getMappedFiles() {
        return mappedFiles;
    }

    public int getMappedFileSize() {
        return mappedFileSize;
    }

    public long getCommittedWhere() {
        return committedWhere;
    }

    public void setCommittedWhere(final long committedWhere) {
        this.committedWhere = committedWhere;
    }

    public long getTotalFileSize() {
        return (long) mappedFileSize * mappedFiles.size();
    }

    public String getStorePath() {
        return storePath;
    }

    public List<MappedFile> range(final long from, final long to) {
        Object[] mfs = copyMappedFiles(0);
        if (null == mfs) {
            return new ArrayList<>();
        }

        List<MappedFile> result = new ArrayList<>();
        for (Object mf : mfs) {
            MappedFile mappedFile = (MappedFile) mf;
            if (mappedFile.getFileFromOffset() + mappedFile.getFileSize() <= from) {
                continue;
            }

            if (to <= mappedFile.getFileFromOffset()) {
                break;
            }
            result.add(mappedFile);
        }

        return result;
    }
}
