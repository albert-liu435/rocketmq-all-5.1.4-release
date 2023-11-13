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
    // 读数据缓冲区大小 4MB
    private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024 * 4;
    //master地址
    private final AtomicReference<String> masterHaAddress = new AtomicReference<>();
    private final AtomicReference<String> masterAddress = new AtomicReference<>();
    //Slave向Master发起主从同步的拉取偏移量，固定8个字节
    // 从节点收到数据后会返回一个 8 字节的 ack 偏移量
    private final ByteBuffer reportOffset = ByteBuffer.allocate(REPORT_HEADER_SIZE);
    // 与 Master 的连接通道
    private SocketChannel socketChannel;
    private Selector selector;
    /**
     * 上次同步偏移量的时间戳
     * last time that slave reads date from master.
     */
    private long lastReadTimestamp = System.currentTimeMillis();
    /**
     * 最近写入数据的时间
     * last time that slave reports offset to master.
     */
    private long lastWriteTimestamp = System.currentTimeMillis();

    // 当前上报过的偏移量
    private long currentReportedOffset = 0;
    // 分发的位置
    private int dispatchPosition = 0;
    //读缓冲大小
    private ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    //备份数据缓冲区
    private ByteBuffer byteBufferBackup = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
    private DefaultMessageStore defaultMessageStore;
    private volatile HAConnectionState currentState = HAConnectionState.READY;
    private FlowMonitor flowMonitor;

    public DefaultHAClient(DefaultMessageStore defaultMessageStore) throws IOException {
        //HAClient 在构造方法中会打开多路复用器 Selector
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

    /**
     * 上报Slave最大偏移量
     *
     * @param maxOffset
     * @return
     */
    private boolean reportSlaveMaxOffset(final long maxOffset) {
        this.reportOffset.position(0);
        this.reportOffset.limit(REPORT_HEADER_SIZE);
        this.reportOffset.putLong(maxOffset);
        this.reportOffset.position(0);
        this.reportOffset.limit(REPORT_HEADER_SIZE);

        for (int i = 0; i < 3 && this.reportOffset.hasRemaining(); i++) {
            try {
                //写入数据
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

    /**
     * 读缓冲区 byteBufferRead 没有剩余空间时，就会重新分配。虽然 byteBufferRead 写满了，但可能数据还没处理完，这时会先将备份缓冲区 byteBufferBackup 复位到0，将剩余的数据先写入备份缓冲区中，
     * 然后将 byteBufferRead 和 byteBufferBackup 交换，再将处理位置 dispatchPosition 改为 0，之后 byteBufferRead 可以继续接收消息，然后从 0 开始处理消息。
     */
    private void reallocateByteBuffer() {
        int remain = READ_MAX_BUFFER_SIZE - this.dispatchPosition;
        // ByteBuffer 已经写满了，但数据还没分发处理完
        if (remain > 0) {
            this.byteBufferRead.position(this.dispatchPosition);

            this.byteBufferBackup.position(0);
            this.byteBufferBackup.limit(READ_MAX_BUFFER_SIZE);
            // 把剩余数据写入备份的缓冲区中
            this.byteBufferBackup.put(this.byteBufferRead);
        }
        // 交换 byteBufferRead 和 byteBufferBackup
        this.swapByteBuffer();

        this.byteBufferRead.position(remain);
        this.byteBufferRead.limit(READ_MAX_BUFFER_SIZE);
        // 交换过后的 byteBufferRead 要从0开始处理数据
        this.dispatchPosition = 0;
    }

    private void swapByteBuffer() {
        ByteBuffer tmp = this.byteBufferRead;
        this.byteBufferRead = this.byteBufferBackup;
        this.byteBufferBackup = tmp;
    }

    /**
     * 在 HAClient 中，slave 会监听处理来自 master 的消息，它会从连接通道把数据读到 byteBufferRead 中，如果没有读取到数据，也会重试三次。读取到数据后，就会分发读请求去处理。
     * master 写入的请求头占 12字节，消息不超过32KB，slave 接收到后会读到 byteBufferRead 缓冲区中，这个读缓冲区的大小为4MB，所以可以放入很多条来自master的数据。所以 HAClient 会有一个 dispatchPosition 来表示消息处理的位置。
     * 每次处理的消息至少得有12字节，master 会单独发请求头过来，也可能会接着发送消息数据过来。
     * 首先就会读取请求头数据，前8字节的物理偏移量masterPhyOffset，接着4字节的消息长度bodySize。然后判断当前slave已经写入的物理偏移量slavePhyOffset，与master当前传输的偏移量(masterPhyOffset)是否一致，如果不一致，说明数据同步有问题，这时就会返回 false，返回 false 之后就会关闭连接通道，之后重新连接 master，重新建立连接时master中的 HAConnection 也会重建，然后 slave 会重新上报当前的偏移量，那么 WriteSocketService 中的 nextTransferFromWhere 就会更新为 slave 节点的物理偏移量，达到主从偏移量同步的目的。
     * 接着就是将消息体追加到 CommitLog 中，进去可以看到就是将消息体数据写入 MappedFile 的 MappedByteBuffer 中。写完之后就更新 dispatchPosition，表示当前已处理消息的位置，下一次就从这个位置开始处理消息。
     *
     * @return
     */
    private boolean processReadEvent() {
        int readSizeZeroTimes = 0;
        //如果读取缓存还有没读取完，则一直读取
        while (this.byteBufferRead.hasRemaining()) {
            try {
                // 读取数据
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
        // salve 节点 CommitLog 的最大偏移量
        long currentPhyOffset = this.defaultMessageStore.getMaxPhyOffset();
        // CommitLog 写入的偏移量已经大于上报的偏移量
        if (currentPhyOffset > this.currentReportedOffset) {
            // 直接更新为 CommitLog 的偏移量
            this.currentReportedOffset = currentPhyOffset;
            // 超出上报偏移量的时间，上报 ack 偏移量
            // 然后重新上报偏移量
            result = this.reportSlaveMaxOffset(this.currentReportedOffset);
            if (!result) {
                this.closeMaster();
                log.error("HAClient, reportSlaveMaxOffset error, " + this.currentReportedOffset);
            }
        }

        return result;
    }

    /**
     * 更改HAConnection状态
     *
     * @param currentState
     */
    public void changeCurrentState(HAConnectionState currentState) {
        log.info("change state to {}", currentState);
        this.currentState = currentState;
    }

    /**
     * 连接Master
     * HAClient 在构造方法中会打开多路复用器 Selector，在connectMaster方法中，就会去连接master地址（masterAddress），拿到连接通道 SocketChannel，然后注册到多路复用器 Selector 上，开始监听 READ 读事件。
     * 可以看到，AcceptSocketService 中是监听 ACCEPT 建立连接事件，HAClient 则监听 READ 读消息事件，也就是说主从复制是有 master 节点发送数据给 slave 去同步，然后 slave 上报同步的偏移量。
     *
     * @return
     * @throws ClosedChannelException
     */
    public boolean connectMaster() throws ClosedChannelException {
        if (null == socketChannel) {
            //master地址
            String addr = this.masterHaAddress.get();
            if (addr != null) {
                //SocketAddress
                SocketAddress socketAddress = NetworkUtil.string2SocketAddress(addr);
                //连接master地址
                this.socketChannel = RemotingHelper.connect(socketAddress);
                if (this.socketChannel != null) {
//                    拿到连接通道 SocketChannel，然后注册到多路复用器 Selector 上，开始监听 READ 读事件
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
     * <p>
     * 连接master，如果当前的broker角色是master，那么对应的masterAddress是空的，不会有后续逻辑。如果是slave，并且配置了master地址，则会进行连接进行后续逻辑处理
     * 检查是否需要向master汇报当前的同步进度，如果两次同步的时间小于5s，则不进行同步。每次同步之间间隔在5s以上，这个5s是心跳连接的间隔参数为haSendHeartbeatInterval
     * 向master 汇报当前 salve 的CommitLog的最大偏移量,并记录这次的同步时间
     * 从master拉取日志信息，主要就是进行消息的同步，同步出问题则关闭连接
     * 再次同步slave的偏移量，如果最新的偏移量大于已经汇报的情况下则从步骤1重头开始
     * <p>
     * HAClient 也是一个 ServiceThread：
     * <p>
     * HAClient 首先会去连接 master 节点，如果连接超时，或是其它网络情况等原因导致连接中断，HAClient 就会每隔 5 秒重新连接 master 节点。
     * <p>
     * <p>
     * 连接到 master 后，先判断是否到了上报偏移量的时间，这个间隔时间由 haSendHeartbeatInterval 配置，即发送心跳的间隔时间，默认是 5秒。
     * <p>
     * <p>
     * 上报完偏移量后，Selector 开始监听 master 的消息，监听到消息后开始处理读消息事件，其实就是将 master 发过来的消息再写入 CommitLog。
     * <p>
     * <p>
     * 本地写入消息后，重新上报当前 CommitLog 的最大偏移量，表示现在同步到了哪里。
     * <p>
     * <p>
     * 最后会判断下消息处理的时间，如果超过 5秒，就会认为网络连接有问题，就关系对 master 的连接，之后会再重新连接 master。
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
                // 上报时间超过5秒，关闭连接
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
        // 超出上报偏移量的时间，上报 ack 偏移量
        if (this.isTimeToReportOffset()) {
            log.info("Slave report current offset {}", this.currentReportedOffset);
            result = this.reportSlaveMaxOffset(this.currentReportedOffset);
            if (!result) {
                return false;
            }
        }
        // 等待消息
        this.selector.select(1000);
        // 处理读事件
        result = this.processReadEvent();
        if (!result) {
            return false;
        }
        //写完数据后，会重新上报偏移量，进去可以看到，主要是判断 CommitLog 已写入的偏移量如果大于当前上报的偏移量currentReportedOffset，
        // 就会更新 currentReportedOffset 为 CommitLog 的偏移量，然后上报给 master 节点。而上报 slave 偏移量其实就是向连接通道写入8字节的偏移量长度。
        // 上报slave最大偏移量(CommitLog 当前偏移量)
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
