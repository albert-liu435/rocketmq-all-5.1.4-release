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
package org.apache.rocketmq.store.logfile;

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.commons.lang3.SystemUtils;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.message.MessageExtBrokerInner;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.AppendMessageCallback;
import org.apache.rocketmq.store.AppendMessageResult;
import org.apache.rocketmq.store.AppendMessageStatus;
import org.apache.rocketmq.store.CompactionAppendMsgCallback;
import org.apache.rocketmq.store.PutMessageContext;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.TransientStorePool;
import org.apache.rocketmq.store.config.FlushDiskType;
import org.apache.rocketmq.store.util.LibC;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

/**
 * https://www.jianshu.com/p/2aad2044980d
 * MapedFile是与RocketMQ的文件模块中最底层得到对象，提供了对文件记录的一些操作方法。后面就对这个类重要属性和方法进行分析。
 */

/*
 *
 *
 *
 *
 *  |       IndexService        |ConsumeQueue|CommitLog |
 *  |       Index File          |   MappedFileQueue     |
 *  |                   MappedFile                      |
 *  |                 MappedByteBuffer                  |
 *  |                      磁盘                          |
 *
 */

public class DefaultMappedFile extends AbstractMappedFile {

    //这里需要额外讲解的是，几个表示位置的参数。wrotePosition，committedPosition，flushedPosition。
    // 大概的关系如下wrotePosition<=committedPosition<=flushedPosition<=fileSize

    //操作系统每页大小，默认4k
    public static final int OS_PAGE_SIZE = 1024 * 4;
    public static final Unsafe UNSAFE = getUnsafe();
    private static final Method IS_LOADED_METHOD;
    public static final int UNSAFE_PAGE_SIZE = UNSAFE == null ? OS_PAGE_SIZE : UNSAFE.pageSize();

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //文件已使用的映射虚拟内存
    //类变量，所有 MappedFile 实例已使用字节总数。
    //当前JVM实例中MappedFile虚拟内存
    protected static final AtomicLong TOTAL_MAPPED_VIRTUAL_MEMORY = new AtomicLong(0);

    //映射文件个数
    //MappedFile 个数。当前JVM实例中MappedFile对象个数
    protected static final AtomicInteger TOTAL_MAPPED_FILES = new AtomicInteger(0);


    //当前文件的写指针，从0开始(内存映射文件中的写指针)
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> WROTE_POSITION_UPDATER;

    //当前文件的提交指针，如果开启transientStorePoolEnable，则数据会存储在transientStorePoolEnable中，然后提交到内存映射ByteBuffer中再写入磁盘
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> COMMITTED_POSITION_UPDATER;

    //刷写到磁盘指针，该指针之前的数据持久化到磁盘中
    protected static final AtomicIntegerFieldUpdater<DefaultMappedFile> FLUSHED_POSITION_UPDATER;

    //已经写入的位置
    //当前MappedFile对象当前写指针。
    protected volatile int wrotePosition;
    // 提交完成位置
    //当前提交的指针。
    protected volatile int committedPosition;
    //刷新完成位置
    //当前刷写到磁盘的指针。
    protected volatile int flushedPosition;
    //文件大小
    protected int fileSize;
    //创建MappedByteBuffer用的文件通道。
    protected FileChannel fileChannel;
    /**
     * Message will put to here first, and then reput to FileChannel if writeBuffer is not null.
     * 消息将首先放在这里，如果writeBuffer不为空，则再放到FileChannel。
     * 如果开启了transientStorePoolEnable，消息会写入堆外内存，然后提交到 PageCache 并最终刷写到磁盘。
     * <p>
     * 堆内存ByteBuffer，如果不为空，数据首先将存储在该Buffer中，然后提交到MappedFile对应的内存映射文件Buffer。
     */
    protected ByteBuffer writeBuffer = null;
    //ByteBuffer的缓冲池，堆外内存，transientStorePoolEnable 为 true 时生效。
    protected TransientStorePool transientStorePool = null;
    //文件名
    protected String fileName;
    //文件开始偏移量
    //文件开始offset,文件序号,代表该文件代表的文件偏移量
    protected long fileFromOffset;
    //物理文件
    protected File file;

    //这个类的作用就是，创建一个直接缓冲区而缓冲区的内容是内存中的文件的内容。可以通过直接操作缓冲区的内容，直接操作内存文件的内容。
    // 这个类创建的方式是通过FileChannel.map方式进行创建的。
    //对应操作系统的 PageCache。
    //物理文件对应的内存映射Buffer
    protected MappedByteBuffer mappedByteBuffer;
    //最后一次存储时间戳。
    protected volatile long storeTimestamp = 0;
    //是否是MappedFileQueue队列中第一个文件。
    protected boolean firstCreateInQueue = false;
    private long lastFlushTime = -1L;

    protected MappedByteBuffer mappedByteBufferWaitToClean = null;
    protected long swapMapTime = 0L;
    protected long mappedByteBufferAccessCountSinceLastSwap = 0L;

    /**
     * If this mapped file belongs to consume queue, this field stores store-timestamp of first message referenced
     * by this logical queue.
     */
    private long startTimestamp = -1;

    /**
     * If this mapped file belongs to consume queue, this field stores store-timestamp of last message referenced
     * by this logical queue.
     */
    private long stopTimestamp = -1;

    static {
        //原子操作
        WROTE_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "wrotePosition");
        COMMITTED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "committedPosition");
        FLUSHED_POSITION_UPDATER = AtomicIntegerFieldUpdater.newUpdater(DefaultMappedFile.class, "flushedPosition");

        Method isLoaded0method = null;
        // On the windows platform and openjdk 11 method isLoaded0 always returns false.
        // see https://github.com/AdoptOpenJDK/openjdk-jdk11/blob/19fb8f93c59dfd791f62d41f332db9e306bc1422/src/java.base/windows/native/libnio/MappedByteBuffer.c#L34
        if (!SystemUtils.IS_OS_WINDOWS) {
            try {
                isLoaded0method = MappedByteBuffer.class.getDeclaredMethod("isLoaded0", long.class, long.class, int.class);
                isLoaded0method.setAccessible(true);
            } catch (NoSuchMethodException ignore) {
            }
        }
        IS_LOADED_METHOD = isLoaded0method;
    }

    public DefaultMappedFile() {
    }

    public DefaultMappedFile(final String fileName, final int fileSize) throws IOException {
        init(fileName, fileSize);
    }

    public DefaultMappedFile(final String fileName, final int fileSize,
                             final TransientStorePool transientStorePool) throws IOException {
        init(fileName, fileSize, transientStorePool);
    }

    public static int getTotalMappedFiles() {
        return TOTAL_MAPPED_FILES.get();
    }

    public static long getTotalMappedVirtualMemory() {
        return TOTAL_MAPPED_VIRTUAL_MEMORY.get();
    }

    /**
     * 创建文件的初始化方法
     * 这个方法是在使用临时存储池时，创建MapedFile对象会指定他的writeBuffer属性指向的是堆外内存。
     *
     * @param fileName           file name
     * @param fileSize           file size
     * @param transientStorePool transient store pool
     * @throws IOException
     */
    @Override
    public void init(final String fileName, final int fileSize,
                     final TransientStorePool transientStorePool) throws IOException {
        //这个重载的init方法
        init(fileName, fileSize);
        //不同就是这里的writeBuffer会被赋值，后续写入操作会优先
        //写入writeBuffer中
        //从临时存储池中获取buffer
        //创建MapedFile对象会指定他的writeBuffer属性指向的是堆外内存。
        this.writeBuffer = transientStorePool.borrowBuffer();
        //记录transientStorePool主要为了释放时归还借用的ByteBuffer
        this.transientStorePool = transientStorePool;
    }

    /**
     * init方法在创建MapedFile对象的时候会调用，在其构造器中调用的。主要作用就是创建对应的文件以及获取对应的文件的映射对象。
     *
     * @param fileName
     * @param fileSize
     * @throws IOException
     */
    private void init(final String fileName, final int fileSize) throws IOException {
        //文件名
        this.fileName = fileName;
        //文件大小
        this.fileSize = fileSize;
        //创建文件
        this.file = new File(fileName);
        //根据文件名称计算文件的真实偏移量
        this.fileFromOffset = Long.parseLong(this.file.getName());
        boolean ok = false;

        UtilAll.ensureDirOK(this.file.getParent());

        try {
            //创建读写类型的fileChannel
            this.fileChannel = new RandomAccessFile(this.file, "rw").getChannel();
            //获取写入类型的内存文件映射对象mappedByteBuffer
            this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            //增加已经映射的虚拟内存
            TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(fileSize);
            //已经映射文件数量+1
            TOTAL_MAPPED_FILES.incrementAndGet();
            ok = true;
        } catch (FileNotFoundException e) {
            log.error("Failed to create file " + this.fileName, e);
            throw e;
        } catch (IOException e) {
            log.error("Failed to map file " + this.fileName, e);
            throw e;
        } finally {
            if (!ok && this.fileChannel != null) {
                this.fileChannel.close();
            }
        }
    }

    @Override
    public boolean renameTo(String fileName) {
        File newFile = new File(fileName);
        boolean rename = file.renameTo(newFile);
        if (rename) {
            this.fileName = fileName;
            this.file = newFile;
        }
        return rename;
    }

    @Override
    public long getLastModifiedTimestamp() {
        return this.file.lastModified();
    }

    public boolean getData(int pos, int size, ByteBuffer byteBuffer) {
        if (byteBuffer.remaining() < size) {
            return false;
        }

        int readPosition = getReadPosition();
        if ((pos + size) <= readPosition) {

            if (this.hold()) {
                try {
                    int readNum = fileChannel.read(byteBuffer, pos);
                    return size == readNum;
                } catch (Throwable t) {
                    log.warn("Get data failed pos:{} size:{} fileFromOffset:{}", pos, size, this.fileFromOffset);
                    return false;
                } finally {
                    this.release();
                }
            } else {
                log.debug("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }

        return false;
    }

    @Override
    public int getFileSize() {
        return fileSize;
    }

    @Override
    public FileChannel getFileChannel() {
        return fileChannel;
    }

    /**
     * @param byteBufferMsg
     * @param cb
     * @return
     */
    public AppendMessageResult appendMessage(final ByteBuffer byteBufferMsg, final CompactionAppendMsgCallback cb) {
        assert byteBufferMsg != null;
        assert cb != null;

        int currentPos = WROTE_POSITION_UPDATER.get(this);
        if (currentPos < this.fileSize) {
            ByteBuffer byteBuffer = appendMessageBuffer().slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result = cb.doAppend(byteBuffer, this.fileFromOffset, this.fileSize - currentPos, byteBufferMsg);
            WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    /**
     * 拼接消息的方法
     * <p>
     * <p>
     * 追加消息到当前的MappedFile,并执行回调函数
     *
     * @param msg               a message to append
     * @param cb                the specific call back to execute the real append action
     * @param putMessageContext
     * @return
     */
    @Override
    public AppendMessageResult appendMessage(final MessageExtBrokerInner msg, final AppendMessageCallback cb,
                                             PutMessageContext putMessageContext) {
        return appendMessagesInner(msg, cb, putMessageContext);
    }

    @Override
    public AppendMessageResult appendMessages(final MessageExtBatch messageExtBatch, final AppendMessageCallback cb,
                                              PutMessageContext putMessageContext) {
        return appendMessagesInner(messageExtBatch, cb, putMessageContext);
    }

    public AppendMessageResult appendMessagesInner(final MessageExt messageExt, final AppendMessageCallback cb,
                                                   PutMessageContext putMessageContext) {
        //断言
        assert messageExt != null;
        assert cb != null;
        //获取当前想写的位置
        int currentPos = WROTE_POSITION_UPDATER.get(this);
        //当前写的位置小于文件的大小
        if (currentPos < this.fileSize) {
            //这里的writeBuffer，如果在启动的时候配置了启用暂存池，这里的writeBuffer是堆外内存方式。获取byteBuffer
            ByteBuffer byteBuffer = appendMessageBuffer().slice();
            byteBuffer.position(currentPos);
            AppendMessageResult result;
            if (messageExt instanceof MessageExtBatch && !((MessageExtBatch) messageExt).isInnerBatch()) {
                //                消息序列化后组装映射的buffer
                // traditional batch message
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBatch) messageExt, putMessageContext);
            } else if (messageExt instanceof MessageExtBrokerInner) {
                // traditional single message or newly introduced inner-batch message
                result = cb.doAppend(this.getFileFromOffset(), byteBuffer, this.fileSize - currentPos,
                        (MessageExtBrokerInner) messageExt, putMessageContext);
            } else {
                return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
            }
            WROTE_POSITION_UPDATER.addAndGet(this, result.getWroteBytes());
            this.storeTimestamp = result.getStoreTimestamp();
            return result;
        }
        log.error("MappedFile.appendMessage return null, wrotePosition: {} fileSize: {}", currentPos, this.fileSize);
        return new AppendMessageResult(AppendMessageStatus.UNKNOWN_ERROR);
    }

    protected ByteBuffer appendMessageBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return writeBuffer != null ? writeBuffer : this.mappedByteBuffer;
    }

    @Override
    public long getFileFromOffset() {
        return this.fileFromOffset;
    }

    @Override
    public boolean appendMessage(final byte[] data) {
        return appendMessage(data, 0, data.length);
    }

    /**
     * 直接将二进制信息通过fileChannel拼接到文件中
     *
     * @param data the byte buffer to append
     * @return
     */
    @Override
    public boolean appendMessage(ByteBuffer data) {
        int currentPos = WROTE_POSITION_UPDATER.get(this);
        int remaining = data.remaining();

        if ((currentPos + remaining) <= this.fileSize) {
            try {
                //设置写的起始位置
                this.fileChannel.position(currentPos);
                while (data.hasRemaining()) {
                    //写入
                    this.fileChannel.write(data);
                }
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            WROTE_POSITION_UPDATER.addAndGet(this, remaining);
            return true;
        }
        return false;
    }

    /**
     * Content of data from offset to offset + length will be written to file.
     *
     * @param offset The offset of the subarray to be used.
     * @param length The length of the subarray to be used.
     */
    @Override
    public boolean appendMessage(final byte[] data, final int offset, final int length) {
        int currentPos = WROTE_POSITION_UPDATER.get(this);

        if ((currentPos + length) <= this.fileSize) {
            try {
                ByteBuffer buf = this.mappedByteBuffer.slice();
                buf.position(currentPos);
                buf.put(data, offset, length);
            } catch (Throwable e) {
                log.error("Error occurred when append message to mappedFile.", e);
            }
            WROTE_POSITION_UPDATER.addAndGet(this, length);
            return true;
        }

        return false;
    }

    /**
     * flush方法比较简单，就是将fileChannel中的数据写入文件中。
     * <p>
     * 刷盘指的是将内存中的数据写入磁盘，永久存储在磁盘中，由
     * MappedFile的flush()方法实现
     * <p>
     * 如果使用writeBuffer存储的话则调用fileChannel的force将内存中的数据持久化到磁盘，刷盘结束后，flushedPosition会等于committedPosition，否则调用mappedByteBuffer的force，最后flushedPosition会等于writePosition。
     *
     * @return The current flushed position
     */

    //直接调用mappedByteBuffer或fileChannel的force()方法将数据
    //写入磁盘，将内存中的数据持久化到磁盘中，那么flushedPosition应
    //该等于MappedByteBuffer中的写指针。如果writeBuffer不为空，则
    //flushedPosition应等于上一次commit指针。因为上一次提交的数据就
    //是进入MappedByteBuffer中的数据。如果writeBuffer为空，表示数据
    //是直接进入MappedByteBuffer的，wrotePosition代表的是
    //MappedByteBuffer中的指针，故设置flushedPosition为
    //wrotePosition。
    @Override
    public int flush(final int flushLeastPages) {
        //判断是否可以进行flush
        if (this.isAbleToFlush(flushLeastPages)) {
            //检查文件是否有效，也就是有引用，并添加引用
            if (this.hold()) {
                //获取写入的位置
                int value = getReadPosition();

                try {
                    this.mappedByteBufferAccessCountSinceLastSwap++;

                    //We only append data to fileChannel or mappedByteBuffer, never both.
                    //如果writeBuffer不为null，说明用了临时存储池，说明前面已经把信息写入了writeBuffer了，直接刷新到磁盘就可以。
                    //fileChannel的位置不为0，说明已经设置了buffer进去了，直接刷新到磁盘
                    if (writeBuffer != null || this.fileChannel.position() != 0) {
                        this.fileChannel.force(false);
                    } else {
                        //如果数据在mappedByteBuffer中，则刷新mappedByteBuffer数据到磁盘
                        this.mappedByteBuffer.force();
                    }
                    this.lastFlushTime = System.currentTimeMillis();
                } catch (Throwable e) {
                    log.error("Error occurred when force data to disk.", e);
                }
                //设置已经刷新的值
                FLUSHED_POSITION_UPDATER.set(this, value);
                //释放引用
                this.release();
            } else {
                log.warn("in flush, hold failed, flush offset = " + FLUSHED_POSITION_UPDATER.get(this));
                FLUSHED_POSITION_UPDATER.set(this, getReadPosition());
            }
        }
        return this.getFlushedPosition();
    }

    /**
     *  这里的commit方法的作用就是把前面写到缓冲中的数据提交到fileChannel中。这里存在两种情况，一种是使用堆外内存的缓冲，一种是使用内存映射的缓冲。两者的处理方式是不一样的。
     *
     * @param commitLeastPages the least pages to commit
     * @return
     */
    @Override
    public int commit(final int commitLeastPages) {
        /**
         * writeBuffer 为  null的情况下，说明没有使用临时存储池，使用的是mappedByteBuffer也就是内存映射的方式，
         * 直接写到映射区域中的，那么这个时候就不需要写入的fileChannel了。直接返回写入的位置作为已经提交的位置。
         *
         * writeBuffer 不为  null，说明用的是临时存储池，使用的堆外内存，那么这个时候需要先把信息提交到fileChannel中
         */
        //如果为空，说明不是堆外内存，就不需要任何操作，只需等待刷盘即可
        if (writeBuffer == null) {
            //no need to commit data to file channel, so just regard wrotePosition as committedPosition.
            return WROTE_POSITION_UPDATER.get(this);
        }

        //no need to commit data to file channel, so just set committedPosition to wrotePosition.
        if (transientStorePool != null && !transientStorePool.isRealCommit()) {
            COMMITTED_POSITION_UPDATER.set(this, WROTE_POSITION_UPDATER.get(this));
            //检查是否需要刷盘
        } else if (this.isAbleToCommit(commitLeastPages)) {
            //检查当前文件是不是有效，就是当前文件还存在引用
            if (this.hold()) {
                //如果是堆外内存，那么需要做commit
                commit0();
                //引用次数减1
                this.release();
            } else {
                log.warn("in commit, hold failed, commit offset = " + COMMITTED_POSITION_UPDATER.get(this));
            }
        }

        // All dirty data has been committed to FileChannel.
        if (writeBuffer != null && this.transientStorePool != null && this.fileSize == COMMITTED_POSITION_UPDATER.get(this)) {
            this.transientStorePool.returnBuffer(writeBuffer);
            this.writeBuffer = null;
        }
        //获取已经刷新的位置
        return COMMITTED_POSITION_UPDATER.get(this);
    }

    protected void commit0() {
        //获取已经写入的数据的位置
        int writePos = WROTE_POSITION_UPDATER.get(this);
        //获取上次提交的位置
        int lastCommittedPosition = COMMITTED_POSITION_UPDATER.get(this);
        //如果还有没有提交的数据，则进行写入
        if (writePos - lastCommittedPosition > 0) {
            try {
                //获取ByteBuffer
                //获取堆外内存
                ByteBuffer byteBuffer = writeBuffer.slice();
                byteBuffer.position(lastCommittedPosition);
                byteBuffer.limit(writePos);
                this.fileChannel.position(lastCommittedPosition);
                //写入fileChannel
                this.fileChannel.write(byteBuffer);
                COMMITTED_POSITION_UPDATER.set(this, writePos);
            } catch (Throwable e) {
                log.error("Error occurred when commit data to FileChannel.", e);
            }
        }
    }

    private boolean isAbleToFlush(final int flushLeastPages) {
        int flush = FLUSHED_POSITION_UPDATER.get(this);
        int write = getReadPosition();

        if (this.isFull()) {
            return true;
        }

        if (flushLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE)) >= flushLeastPages;
        }

        return write > flush;
    }

    protected boolean isAbleToCommit(final int commitLeastPages) {
        int commit = COMMITTED_POSITION_UPDATER.get(this);
        int write = WROTE_POSITION_UPDATER.get(this);

        if (this.isFull()) {
            return true;
        }

        if (commitLeastPages > 0) {
            return ((write / OS_PAGE_SIZE) - (commit / OS_PAGE_SIZE)) >= commitLeastPages;
        }

        return write > commit;
    }

    @Override
    public int getFlushedPosition() {
        return FLUSHED_POSITION_UPDATER.get(this);
    }

    @Override
    public void setFlushedPosition(int pos) {
        FLUSHED_POSITION_UPDATER.set(this, pos);
    }

    @Override
    public boolean isFull() {
        return this.fileSize == WROTE_POSITION_UPDATER.get(this);
    }

    /**
     * 读取数据 指定位置和读取的长度
     *
     * @param pos  the given position
     * @param size the size of the returned sub-region
     * @return
     */
    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos, int size) {
        //获取提交的位置
        int readPosition = getReadPosition();
        //如果要读取的信息在已经提交的信息中，就进行读取
        if ((pos + size) <= readPosition) {
            //检查文件是否有效
            if (this.hold()) {
                this.mappedByteBufferAccessCountSinceLastSwap++;
                //读取数据然后返回
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            } else {
                log.warn("matched, but hold failed, request pos: " + pos + ", fileFromOffset: "
                        + this.fileFromOffset);
            }
        } else {
            log.warn("selectMappedBuffer request pos invalid, request pos: " + pos + ", size: " + size
                    + ", fileFromOffset: " + this.fileFromOffset);
        }

        return null;
    }

    /**
     * 读取数据 读取指定位置后的所有数据。
     *
     * @param pos the given position
     * @return
     */
    @Override
    public SelectMappedBufferResult selectMappedBuffer(int pos) {
        //获取文件读取的位置
        int readPosition = getReadPosition();
        if (pos < readPosition && pos >= 0) {
            if (this.hold()) {
                this.mappedByteBufferAccessCountSinceLastSwap++;
                //创建新的缓冲区
                ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
                byteBuffer.position(pos);
                //获取指定位置到最新提交的位置之间的数据
                // 得到要读取数据的大小
                int size = readPosition - pos;
                // 剩余的 ByteBuffer
                ByteBuffer byteBufferNew = byteBuffer.slice();
                byteBufferNew.limit(size);
                return new SelectMappedBufferResult(this.fileFromOffset + pos, byteBufferNew, size, this);
            }
        }

        return null;
    }

    /**
     * 清除内存映射
     *
     * @param currentRef
     * @return
     */
    @Override
    public boolean cleanup(final long currentRef) {

        if (this.isAvailable()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have not shutdown, stop unmapping.");
            return false;
        }

        if (this.isCleanupOver()) {
            log.error("this file[REF:" + currentRef + "] " + this.fileName
                    + " have cleanup, do not do it again.");
            return true;
        }

        //        清除映射缓冲区=》
        UtilAll.cleanBuffer(this.mappedByteBuffer);
        UtilAll.cleanBuffer(this.mappedByteBufferWaitToClean);
        this.mappedByteBufferWaitToClean = null;
        //        减少映射文件所占虚拟内存
        TOTAL_MAPPED_VIRTUAL_MEMORY.addAndGet(this.fileSize * (-1));
        //        改变映射文件数量
        TOTAL_MAPPED_FILES.decrementAndGet();
        log.info("unmap file[REF:" + currentRef + "] " + this.fileName + " OK");
        return true;
    }

    @Override
    public boolean destroy(final long intervalForcibly) {
        //        =》删除引用
        this.shutdown(intervalForcibly);

        //已经清除了文件的引用
        if (this.isCleanupOver()) {
            try {
                long lastModified = getLastModifiedTimestamp();
                //                关闭文件channel
                this.fileChannel.close();
                log.info("close file channel " + this.fileName + " OK");

                long beginTime = System.currentTimeMillis();
                //                删除文件
                boolean result = this.file.delete();
                log.info("delete file[REF:" + this.getRefCount() + "] " + this.fileName
                        + (result ? " OK, " : " Failed, ") + "W:" + this.getWrotePosition() + " M:"
                        + this.getFlushedPosition() + ", "
                        + UtilAll.computeElapsedTimeMilliseconds(beginTime)
                        + "," + (System.currentTimeMillis() - lastModified));
            } catch (Exception e) {
                log.warn("close file channel " + this.fileName + " Failed. ", e);
            }

            return true;
        } else {
            log.warn("destroy mapped file[REF:" + this.getRefCount() + "] " + this.fileName
                    + " Failed. cleanupOver: " + this.cleanupOver);
        }

        return false;
    }

    @Override
    public int getWrotePosition() {
        return WROTE_POSITION_UPDATER.get(this);
    }

    @Override
    public void setWrotePosition(int pos) {
        WROTE_POSITION_UPDATER.set(this, pos);
    }

    /**
     * @return The max position which have valid data
     */
    @Override
    public int getReadPosition() {
        return transientStorePool == null || !transientStorePool.isRealCommit() ? WROTE_POSITION_UPDATER.get(this) : COMMITTED_POSITION_UPDATER.get(this);
    }

    @Override
    public void setCommittedPosition(int pos) {
        COMMITTED_POSITION_UPDATER.set(this, pos);
    }

    @Override
    public void warmMappedFile(FlushDiskType type, int pages) {
        this.mappedByteBufferAccessCountSinceLastSwap++;

        long beginTime = System.currentTimeMillis();
        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        long flush = 0;
        // long time = System.currentTimeMillis();
        for (long i = 0, j = 0; i < this.fileSize; i += DefaultMappedFile.OS_PAGE_SIZE, j++) {
            byteBuffer.put((int) i, (byte) 0);
            // force flush when flush disk type is sync
            if (type == FlushDiskType.SYNC_FLUSH) {
                if ((i / OS_PAGE_SIZE) - (flush / OS_PAGE_SIZE) >= pages) {
                    flush = i;
                    mappedByteBuffer.force();
                }
            }

            // prevent gc
            // if (j % 1000 == 0) {
            //     log.info("j={}, costTime={}", j, System.currentTimeMillis() - time);
            //     time = System.currentTimeMillis();
            //     try {
            //         Thread.sleep(0);
            //     } catch (InterruptedException e) {
            //         log.error("Interrupted", e);
            //     }
            // }
        }

        // force flush when prepare load finished
        if (type == FlushDiskType.SYNC_FLUSH) {
            log.info("mapped file warm-up done, force to disk, mappedFile={}, costTime={}",
                    this.getFileName(), System.currentTimeMillis() - beginTime);
            mappedByteBuffer.force();
        }
        log.info("mapped file warm-up done. mappedFile={}, costTime={}", this.getFileName(),
                System.currentTimeMillis() - beginTime);

        this.mlock();
    }

    @Override
    public boolean swapMap() {
        if (getRefCount() == 1 && this.mappedByteBufferWaitToClean == null) {

            if (!hold()) {
                log.warn("in swapMap, hold failed, fileName: " + this.fileName);
                return false;
            }
            try {
                this.mappedByteBufferWaitToClean = this.mappedByteBuffer;
                this.mappedByteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
                this.mappedByteBufferAccessCountSinceLastSwap = 0L;
                this.swapMapTime = System.currentTimeMillis();
                log.info("swap file " + this.fileName + " success.");
                return true;
            } catch (Exception e) {
                log.error("swapMap file " + this.fileName + " Failed. ", e);
            } finally {
                this.release();
            }
        } else {
            log.info("Will not swap file: " + this.fileName + ", ref=" + getRefCount());
        }
        return false;
    }

    @Override
    public void cleanSwapedMap(boolean force) {
        try {
            if (this.mappedByteBufferWaitToClean == null) {
                return;
            }
            long minGapTime = 120 * 1000L;
            long gapTime = System.currentTimeMillis() - this.swapMapTime;
            if (!force && gapTime < minGapTime) {
                Thread.sleep(minGapTime - gapTime);
            }
            UtilAll.cleanBuffer(this.mappedByteBufferWaitToClean);
            mappedByteBufferWaitToClean = null;
            log.info("cleanSwapedMap file " + this.fileName + " success.");
        } catch (Exception e) {
            log.error("cleanSwapedMap file " + this.fileName + " Failed. ", e);
        }
    }

    @Override
    public long getRecentSwapMapTime() {
        return 0;
    }

    @Override
    public long getMappedByteBufferAccessCountSinceLastSwap() {
        return this.mappedByteBufferAccessCountSinceLastSwap;
    }

    @Override
    public long getLastFlushTime() {
        return this.lastFlushTime;
    }

    @Override
    public String getFileName() {
        return fileName;
    }

    @Override
    public MappedByteBuffer getMappedByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return mappedByteBuffer;
    }

    @Override
    public ByteBuffer sliceByteBuffer() {
        this.mappedByteBufferAccessCountSinceLastSwap++;
        return this.mappedByteBuffer.slice();
    }

    @Override
    public long getStoreTimestamp() {
        return storeTimestamp;
    }

    @Override
    public boolean isFirstCreateInQueue() {
        return firstCreateInQueue;
    }

    @Override
    public void setFirstCreateInQueue(boolean firstCreateInQueue) {
        this.firstCreateInQueue = firstCreateInQueue;
    }

    @Override
    public void mlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        {
            int ret = LibC.INSTANCE.mlock(pointer, new NativeLong(this.fileSize));
            log.info("mlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }

        {
            int ret = LibC.INSTANCE.madvise(pointer, new NativeLong(this.fileSize), LibC.MADV_WILLNEED);
            log.info("madvise {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
        }
    }

    @Override
    public void munlock() {
        final long beginTime = System.currentTimeMillis();
        final long address = ((DirectBuffer) (this.mappedByteBuffer)).address();
        Pointer pointer = new Pointer(address);
        int ret = LibC.INSTANCE.munlock(pointer, new NativeLong(this.fileSize));
        log.info("munlock {} {} {} ret = {} time consuming = {}", address, this.fileName, this.fileSize, ret, System.currentTimeMillis() - beginTime);
    }

    @Override
    public File getFile() {
        return this.file;
    }

    @Override
    public void renameToDelete() {
        //use Files.move
        if (!fileName.endsWith(".delete")) {
            String newFileName = this.fileName + ".delete";
            try {
                Path newFilePath = Paths.get(newFileName);
                // https://bugs.openjdk.org/browse/JDK-4724038
                // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
                // Windows can't move the file when mmapped.
                if (NetworkUtil.isWindowsPlatform() && mappedByteBuffer != null) {
                    long position = this.fileChannel.position();
                    UtilAll.cleanBuffer(this.mappedByteBuffer);
                    this.fileChannel.close();
                    Files.move(Paths.get(fileName), newFilePath, StandardCopyOption.ATOMIC_MOVE);
                    try (RandomAccessFile file = new RandomAccessFile(newFileName, "rw")) {
                        this.fileChannel = file.getChannel();
                        this.fileChannel.position(position);
                        this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
                    }
                } else {
                    Files.move(Paths.get(fileName), newFilePath, StandardCopyOption.ATOMIC_MOVE);
                }
                this.fileName = newFileName;
                this.file = new File(newFileName);
            } catch (IOException e) {
                log.error("move file {} failed", fileName, e);
            }
        }
    }

    @Override
    public void moveToParent() throws IOException {
        Path currentPath = Paths.get(fileName);
        String baseName = currentPath.getFileName().toString();
        Path parentPath = currentPath.getParent().getParent().resolve(baseName);
        // https://bugs.openjdk.org/browse/JDK-4724038
        // https://bugs.java.com/bugdatabase/view_bug.do?bug_id=4715154
        // Windows can't move the file when mmapped.
        if (NetworkUtil.isWindowsPlatform() && mappedByteBuffer != null) {
            long position = this.fileChannel.position();
            UtilAll.cleanBuffer(this.mappedByteBuffer);
            this.fileChannel.close();
            Files.move(Paths.get(fileName), parentPath, StandardCopyOption.ATOMIC_MOVE);
            try (RandomAccessFile file = new RandomAccessFile(parentPath.toFile(), "rw")) {
                this.fileChannel = file.getChannel();
                this.fileChannel.position(position);
                this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
            }
        } else {
            Files.move(Paths.get(fileName), parentPath, StandardCopyOption.ATOMIC_MOVE);
        }
        this.file = parentPath.toFile();
        this.fileName = parentPath.toString();
    }

    @Override
    public String toString() {
        return this.fileName;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public void setStartTimestamp(long startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    public long getStopTimestamp() {
        return stopTimestamp;
    }

    public void setStopTimestamp(long stopTimestamp) {
        this.stopTimestamp = stopTimestamp;
    }


    public Iterator<SelectMappedBufferResult> iterator(int startPos) {
        return new Itr(startPos);
    }

    public static Unsafe getUnsafe() {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return (Unsafe) f.get(null);
        } catch (Exception ignore) {

        }
        return null;
    }

    public static long mappingAddr(long addr) {
        long offset = addr % UNSAFE_PAGE_SIZE;
        offset = (offset >= 0) ? offset : (UNSAFE_PAGE_SIZE + offset);
        return addr - offset;
    }

    public static int pageCount(long size) {
        return (int) (size + (long) UNSAFE_PAGE_SIZE - 1L) / UNSAFE_PAGE_SIZE;
    }

    @Override
    public boolean isLoaded(long position, int size) {
        if (IS_LOADED_METHOD == null) {
            return true;
        }
        try {
            long addr = ((DirectBuffer) mappedByteBuffer).address() + position;
            return (boolean) IS_LOADED_METHOD.invoke(mappedByteBuffer, mappingAddr(addr), size, pageCount(size));
        } catch (Exception e) {
            log.info("invoke isLoaded0 of file {} error:", file.getAbsolutePath(), e);
        }
        return true;
    }

    private class Itr implements Iterator<SelectMappedBufferResult> {
        private int start;
        private int current;
        private ByteBuffer buf;

        public Itr(int pos) {
            this.start = pos;
            this.current = pos;
            this.buf = mappedByteBuffer.slice();
            this.buf.position(start);
        }

        @Override
        public boolean hasNext() {
            return current < getReadPosition();
        }

        @Override
        public SelectMappedBufferResult next() {
            int readPosition = getReadPosition();
            if (current < readPosition && current >= 0) {
                if (hold()) {
                    ByteBuffer byteBuffer = buf.slice();
                    byteBuffer.position(current);
                    int size = byteBuffer.getInt(current);
                    ByteBuffer bufferResult = byteBuffer.slice();
                    bufferResult.limit(size);
                    current += size;
                    return new SelectMappedBufferResult(fileFromOffset + current, bufferResult, size,
                            DefaultMappedFile.this);
                }
            }
            return null;
        }

        @Override
        public void forEachRemaining(Consumer<? super SelectMappedBufferResult> action) {
            Iterator.super.forEachRemaining(action);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

}
