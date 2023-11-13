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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.netty.NettySystemConfig;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class DefaultHAConnection implements HAConnection {

    /**
     * Transfer Header buffer size. Schema: physic offset and body size. Format:
     *
     * <pre>
     * ┌───────────────────────────────────────────────┬───────────────────────┐
     * │                  physicOffset                 │         bodySize      │
     * │                    (8bytes)                   │         (4bytes)      │
     * ├───────────────────────────────────────────────┴───────────────────────┤
     * │                                                                       │
     * │                           Transfer Header                             │
     * </pre>
     * <p>
     */
    public static final int TRANSFER_HEADER_SIZE = 8 + 4;

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final DefaultHAService haService;
    private final SocketChannel socketChannel;
    private final String clientAddress;
    private WriteSocketService writeSocketService;
    private ReadSocketService readSocketService;
    private volatile HAConnectionState currentState = HAConnectionState.TRANSFER;
    //表示slave请求获取的偏移量，
    private volatile long slaveRequestOffset = -1;
    //slaveAckOffset 表示slave同步数据后ack的偏移量。
    private volatile long slaveAckOffset = -1;
    private FlowMonitor flowMonitor;

    public DefaultHAConnection(final DefaultHAService haService, final SocketChannel socketChannel) throws IOException {
        //指定所属的 HAService
        this.haService = haService;
        //指定的NIO的socketChannel
        this.socketChannel = socketChannel;
        //客户端的地址
        this.clientAddress = this.socketChannel.socket().getRemoteSocketAddress().toString();
        //这是为非阻塞
        this.socketChannel.configureBlocking(false);
        /**
         * 是否启动SO_LINGER
         * SO_LINGER作用
         * 设置函数close()关闭TCP连接时的行为。缺省close()的行为是，如果有数据残留在socket发送缓冲区中则系统将继续发送这些数据给对方，等待被确认，然后返回。
         *
         * https://blog.csdn.net/u012635648/article/details/80279338
         */
        this.socketChannel.socket().setSoLinger(false, -1);
        /**
         * 是否开启TCP_NODELAY
         * https://blog.csdn.net/lclwjl/article/details/80154565
         */
        this.socketChannel.socket().setTcpNoDelay(true);
        if (NettySystemConfig.socketSndbufSize > 0) {
            //接收缓冲的大小
            this.socketChannel.socket().setReceiveBufferSize(NettySystemConfig.socketSndbufSize);
        }
        if (NettySystemConfig.socketRcvbufSize > 0) {
            //发送缓冲的大小
            this.socketChannel.socket().setSendBufferSize(NettySystemConfig.socketRcvbufSize);
        }
        //端口写服务
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        //端口读服务
        this.readSocketService = new ReadSocketService(this.socketChannel);
        //增加haService中的连接数字段
        this.haService.getConnectionCount().incrementAndGet();
        this.flowMonitor = new FlowMonitor(haService.getDefaultMessageStore().getMessageStoreConfig());
    }

    public void start() {
        changeCurrentState(HAConnectionState.TRANSFER);
        this.flowMonitor.start();
        this.readSocketService.start();
        this.writeSocketService.start();
    }

    public void shutdown() {
        changeCurrentState(HAConnectionState.SHUTDOWN);
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.flowMonitor.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    public void changeCurrentState(HAConnectionState currentState) {
        log.info("change state to {}", currentState);
        this.currentState = currentState;
    }

    @Override
    public HAConnectionState getCurrentState() {
        return currentState;
    }

    @Override
    public String getClientAddress() {
        return this.clientAddress;
    }

    @Override
    public long getSlaveAckOffset() {
        return slaveAckOffset;
    }

    public long getTransferredByteInSecond() {
        return this.flowMonitor.getTransferredByteInSecond();
    }

    public long getTransferFromWhere() {
        return writeSocketService.getNextTransferFromWhere();
    }

    /**
     * HAClient 中，slave 节点每次同步数据后，会向 master 上报当前 CommitLog 的物理偏移量，每隔5秒也会上报一次。
     * HAConnection 中的 ReadSocketService 就是用来监听 slave 节点的发送的数据，监听到 READ 事件后，就会调用 processReadEvent() 来处理数据。如果处理READ事件失败或者处理时间超过20秒，就认为与slave的连接超时过期，
     * 当前这个连接就不要了，这时就会关闭相关资源，让slave重新连接。
     * <p>
     * <p>
     * 处理网络连接的读数据
     * <p>
     * 来看master对slave上报偏移量的处理，整体看下来会发现跟 HAClient 处理 READ 事件是类似的。
     * ReadSocketService 用1MB的读缓冲区 byteBufferRead 来缓冲通道的数据，然后用 processPosition 来表示已处理消息的位置，而 HAClient 每次上报偏移量的长度固定为 8字节。
     * 首先如果 byteBufferRead 读缓冲如果写满了，就会做复位（flip()），processPosition 归零。这里和 HAClient 的处理是有区别的，这里如果 byteBufferRead 还有未处理的数据，复位后就直接丢弃了，因为偏移量相对来说没那么重要，反正 slave 至少5秒后会重新上报偏移量。而 slave 因为要同步消息数据，不能丢弃消息数据，所以 slave 会用一个 byteBufferBackup 备份缓冲区来备份剩余的数据。
     * 从连接通道读到数据后，可以看到，它只会处理最后一条消息，读取8字节的偏移量，然后更新到 slaveAckOffset，如果 slaveAckOffset 大于master的最大物理偏移量，说明主从同步有问题，需要slave重新连接同步。如果 slaveRequestOffset = -1，会将其更新为最新的 slave 偏移量。
     */
    class ReadSocketService extends ServiceThread {
        // 读数据缓冲区大小 1M
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        private final Selector selector;
        private final SocketChannel socketChannel;
        // 读数据缓冲区
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        // 处理消息的位置
        private int processPosition = 0;
        // 最近一次读取数据的时间戳
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = NetworkUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        /**
         *  整体的逻辑如下：
         * <p>
         * 每1s执行一次事件就绪选择，然后调用processReadEvent方法处理读请求，读取从服务器的拉取请求
         * 获取slave已拉取偏移量，因为有新的从服务器反馈拉取进度，需要通知某些生产者以便返回，因为如果消息发送使用同步方式，需要等待将消息复制到从服务器，然后才返回，故这里需要唤醒相关线程去判断自己关注的消息是否已经传输完成。也就是HAService的GroupTransferService
         * 如果读取到的字节数等于0，则重复三次，否则结束本次读请求处理；如果读取到的字节数小于0，表示连接被断开，返回false，后续会断开该连接。
         * ————————————————
         * 版权声明：本文为CSDN博主「szhlcy」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
         * 原文链接：https://blog.csdn.net/szhlcy/article/details/116055627
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        log.error("processReadEvent error");
                        break;
                    }


                    long interval = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    // 处理读事件的时间大于20秒
                    if (interval > DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        log.warn("ha housekeeping, found this connection[" + DefaultHAConnection.this.clientAddress + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            changeCurrentState(HAConnectionState.SHUTDOWN);
            // 标记 ServiceThread 停止
            this.makeStop();

            writeSocketService.makeStop();

            // 移除 HAConnection
            haService.removeConnection(DefaultHAConnection.this);

            // 停止网络通道
            DefaultHAConnection.this.haService.getConnectionCount().decrementAndGet();

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                log.error("", e);
            }

            flowMonitor.shutdown(true);

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
                return haService.getDefaultMessageStore().getBrokerIdentity().getIdentifier() + ReadSocketService.class.getSimpleName();
            }
            return ReadSocketService.class.getSimpleName();
        }

        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            if (!this.byteBufferRead.hasRemaining()) {
                this.byteBufferRead.flip();
                this.processPosition = 0;
            }

            while (this.byteBufferRead.hasRemaining()) {
                try {
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        readSizeZeroTimes = 0;
                        this.lastReadTimestamp = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        if ((this.byteBufferRead.position() - this.processPosition) >= DefaultHAClient.REPORT_HEADER_SIZE) {
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % DefaultHAClient.REPORT_HEADER_SIZE);
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            this.processPosition = pos;

                            DefaultHAConnection.this.slaveAckOffset = readOffset;
                            if (DefaultHAConnection.this.slaveRequestOffset < 0) {
                                DefaultHAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + DefaultHAConnection.this.clientAddress + "] request offset " + readOffset);
                            }

                            DefaultHAConnection.this.haService.notifyTransferSome(DefaultHAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        if (++readSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        log.error("read socket[" + DefaultHAConnection.this.clientAddress + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * 处理网络的写数据
     * 监听slave日志同步进度和同步日志的WriteSocketService
     * WriteSocketService监听的是OP_WRITE事件，注册的端口就是在HAService中开启的端口。直接看对应的核心方法run方法，方法有点长这里只看看核心的部分
     */
    class WriteSocketService extends ServiceThread {
        private final Selector selector;
        private final SocketChannel socketChannel;

        //用来写数据头的一个缓冲区，8+4 字节长度
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(TRANSFER_HEADER_SIZE);
        //下一个传输的偏移量
        private long nextTransferFromWhere = -1;
        //这个是从 MappedFile 返回的查询数据对象
        private SelectMappedBufferResult selectMappedBufferResult;
        //最后一次写数据是否完成
        private boolean lastWriteOver = true;
        private long lastPrintTimestamp = System.currentTimeMillis();
        //最后一次写数据的时间戳
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = NetworkUtil.openSelector();
            this.socketChannel = socketChannel;
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        /**
         * 主要的逻辑如下：
         * <p>
         * 如果slave进行了日志偏移量的汇报，判断是不是第一次的进行同步以及对应的同步进度。设置下一次的同步位置
         * 检查上次同步是不是已经完成了，检查两次同步的周期是不是超过心跳间隔，如果是的则需要把心跳信息放到返回的头里面，然后进行消息同步。如果上次同步还没完成，则等待上次同步完成之后再继续
         * 从Master本地读取CommitLog的最大偏移量，根据上次同步的位置开始从CommitLog获取日志信息，然后放到缓存中
         * 如果缓存的大小大于单次同步的最大大小haTransferBatchSize默认是32kb，那么只同步32kb大小的日志。如果缓存为null，则等待100毫秒
         * <p>
         * * 主要的逻辑如下：
         * <p>
         * 首先判断 slaveRequestOffset，正常来说，slave 连接到 master 后，会上报一次自己的偏移量，初始值为 0，所以在还没开始传输数据的时候 slaveRequestOffset = 0。
         * <p>
         * <p>
         * 还没开始传输前，nextTransferFromWhere = -1，这时就会去获取 CommitLog 的最大偏移量 maxOffset，然后计算master的偏移量：masterOffset = maxOffset -  maxOffset % commitLogFileSize，这么算下来masterOffset其实就等于最后一个 MappedFile 的起始偏移量 fileFromOffset。然后将 nextTransferFromWhere 更新为 masterOffset。如果已经同步过了，那么 nextTransferFromWhere > 0， 就会将其更新为 slaveRequestOffset。
         * 这一步就是在更新下一次开始传输的位置，如果是一次传输，那么就从第一个 MappedFile 的起始位置开始，否则就是从slave之前同步的位置开始。
         * 而 CommitLog 的 getMaxOffset() 看源码会发现其实就是获取最后一个 MappedFile 的写入位置，即 maxOffset=fileFromOffset + readPosition，fileFromOffset 就是 MappedFile 的起始偏移量，readPosition 在存在 writeBuffer 时就是 committedPosition，否则就是 wrotePosition。
         * <p>
         * <p>
         * 接着判断上一次写入数据是否完成（lastWriteOver），未完成就继续之前的数据传输。否则会每隔5秒调一次 transferData() 来传输数据，它这里会先向 byteBufferHeader 写入8个字节的 nextTransferFromWhere。
         * <p>
         * <p>
         * 之后会根据 nextTransferFromWhere 去 CommitLog 读取数据，得到 SelectMappedBufferResult，这就是要传输的消息数据。
         * 然后可以看到一次传输的数据不会超过 haTransferBatchSize（32KB）大小，然后将 nextTransferFromWhere 加上这次传输的数据长度，表示下一次要传输的位置。然后更新 byteBufferHeader，前8字节存储当前传输的偏移量，后4字节存储当前传输的数据大小。最后就调用 transferData() 传输数据。
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    this.selector.select(1000);

                    if (-1 == DefaultHAConnection.this.slaveRequestOffset) {
                        Thread.sleep(10);
                        continue;
                    }
                    // 更新下一个传输的偏移量 nextTransferFromWhere
                    if (-1 == this.nextTransferFromWhere) {
                        if (0 == DefaultHAConnection.this.slaveRequestOffset) {
                            long masterOffset = DefaultHAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            masterOffset =
                                    masterOffset
                                            - (masterOffset % DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                            .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            this.nextTransferFromWhere = DefaultHAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + DefaultHAConnection.this.clientAddress
                                + "], and slave request " + DefaultHAConnection.this.slaveRequestOffset);
                    }

                    // 向slave写入传输的开始位置
                    if (this.lastWriteOver) {

                        //间隔5s写入数据
                        long interval =
                                DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        if (interval > DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                .getHaSendHeartbeatInterval()) {

                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(TRANSFER_HEADER_SIZE);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else {
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    // 查询数据，同步数据
                    SelectMappedBufferResult selectResult =
                            DefaultHAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);

                    if (selectResult != null) {
                        int size = selectResult.getSize();
                        if (size > DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            size = DefaultHAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        int canTransferMaxBytes = flowMonitor.canTransferMaxByteNum();
                        if (size > canTransferMaxBytes) {
                            if (System.currentTimeMillis() - lastPrintTimestamp > 1000) {
                                log.warn("Trigger HA flow control, max transfer speed {}KB/s, current speed: {}KB/s",
                                        String.format("%.2f", flowMonitor.maxTransferByteInSecond() / 1024.0),
                                        String.format("%.2f", flowMonitor.getTransferredByteInSecond() / 1024.0));
                                lastPrintTimestamp = System.currentTimeMillis();
                            }
                            size = canTransferMaxBytes;
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        this.nextTransferFromWhere += size;

                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(TRANSFER_HEADER_SIZE);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        this.lastWriteOver = this.transferData();
                    } else {

                        DefaultHAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    DefaultHAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            DefaultHAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            changeCurrentState(HAConnectionState.SHUTDOWN);

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(DefaultHAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                DefaultHAConnection.log.error("", e);
            }

            flowMonitor.shutdown(true);

            DefaultHAConnection.log.info(this.getServiceName() + " service end");
        }

        /**
         * 再来看数据传输部分，首先会向 slave 节点写入请求头，这个请求头已经分析过，包含一个8字节的当前传输的起始偏移量（thisOffset），以及传输的数据大小（size）。如果传输失败，比如网络问题，会重试三次。
         * 传输成功则更新最后一次写入时间 lastWriteTimestamp 为当前时间。
         * 请求头写完之后，就开始写消息数据，如果失败同样也会重试三次，成功则更新 lastWriteTimestamp。
         *
         * @return
         * @throws Exception
         */
        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            // 写入请求头: thisOffset+size
            while (this.byteBufferHeader.hasRemaining()) {
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    flowMonitor.addByteCountTransferred(writeSize);
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            if (null == this.selectMappedBufferResult) {
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            // 写入消息
            if (!this.byteBufferHeader.hasRemaining()) {
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = DefaultHAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            // 请求头和消息体是否都已经传输完成
            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            if (haService.getDefaultMessageStore().getBrokerConfig().isInBrokerContainer()) {
                return haService.getDefaultMessageStore().getBrokerIdentity().getIdentifier() + WriteSocketService.class.getSimpleName();
            }
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }

        public long getNextTransferFromWhere() {
            return nextTransferFromWhere;
        }
    }
}
