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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.utils.NetworkUtil;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.protocol.body.HARuntimeInfo;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;

/**
 *  HAService是在RocketMQ的Broker启动的时候就会创建的，而创建的点在DefaultMessageStore这个消息存储相关的综合类中，在这个类的构造器中会创建HAService无论当前的Broker是什么角色。这个类后续会有文章分析
 *  这里需要说明的是Broker中的Master和Slaver两个角色，代码都是一样的，只不过是在实际执行的时候，走的分支不一样
 * ————————————————
 * 版权声明：本文为CSDN博主「szhlcy」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
 * 原文链接：https://blog.csdn.net/szhlcy/article/details/116055627
 */
public class DefaultHAService implements HAService {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //连接到 master 的slave数量
    protected final AtomicInteger connectionCount = new AtomicInteger(0);

    //主从建立的网络连接，一个 master 可能有多个 slave，每一个 HAConnection 就代表一个 slave 节点
    protected final List<HAConnection> connectionList = new LinkedList<>();

    //用于接收连接用的服务，只监听OP_ACCEPT事件，监听到连接事件时候，创建HAConnection来处理读写请求事件
    protected AcceptSocketService acceptSocketService;

    protected DefaultMessageStore defaultMessageStore;

    //线程阻塞，等待通知组件
    protected WaitNotifyObject waitNotifyObject = new WaitNotifyObject();

    //master跟slave 消息同步的位移量
    //推送到slave的最大偏移量
    protected AtomicLong push2SlaveMaxOffset = new AtomicLong(0);

    //组传输的组件
    protected GroupTransferService groupTransferService;

    //主从同步客户端组件
    protected HAClient haClient;

    protected HAConnectionStateNotificationService haConnectionStateNotificationService;

    public DefaultHAService() {
    }

    @Override
    public void init(final DefaultMessageStore defaultMessageStore) throws IOException {
        this.defaultMessageStore = defaultMessageStore;
        // HA 监听端口 10912
        this.acceptSocketService = new DefaultAcceptSocketService(defaultMessageStore.getMessageStoreConfig());
        this.groupTransferService = new GroupTransferService(this, defaultMessageStore);
        if (this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
            this.haClient = new DefaultHAClient(this.defaultMessageStore);
        }
        this.haConnectionStateNotificationService = new HAConnectionStateNotificationService(this, defaultMessageStore);
    }

    @Override
    public void updateMasterAddress(final String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateMasterAddress(newAddr);
        }
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        if (this.haClient != null) {
            this.haClient.updateHaMasterAddress(newAddr);
        }
    }

    @Override
    public void putRequest(final CommitLog.GroupCommitRequest request) {
        this.groupTransferService.putRequest(request);
    }

    @Override
    public boolean isSlaveOK(final long masterPutWhere) {
        boolean result = this.connectionCount.get() > 0;
        result =
                result
                        && masterPutWhere - this.push2SlaveMaxOffset.get() < this.defaultMessageStore
                        .getMessageStoreConfig().getHaMaxGapNotInSync();
        return result;
    }

    public void notifyTransferSome(final long offset) {
        for (long value = this.push2SlaveMaxOffset.get(); offset > value; ) {
            boolean ok = this.push2SlaveMaxOffset.compareAndSet(value, offset);
            if (ok) {
                this.groupTransferService.notifyTransferSome();
                break;
            } else {
                value = this.push2SlaveMaxOffset.get();
            }
        }
    }

    @Override
    public AtomicInteger getConnectionCount() {
        return connectionCount;
    }

    @Override
    public void start() throws Exception {
        //接受连接的服务，开启端口，设置监听的事件
        this.acceptSocketService.beginAccept();
        //开启服务不断检查是否有连接
        this.acceptSocketService.start();
        //开启groupTransferService，接受CommitLog的主从同步请求
        this.groupTransferService.start();
        this.haConnectionStateNotificationService.start();
        if (haClient != null) {
            //开启haClient，用于slave来建立与Master连接和同步
            this.haClient.start();
        }
    }

    /**
     * 添加连接到列表中
     *
     * @param conn
     */
    public void addConnection(final HAConnection conn) {
        synchronized (this.connectionList) {
            this.connectionList.add(conn);
        }
    }

    public void removeConnection(final HAConnection conn) {
        this.haConnectionStateNotificationService.checkConnectionStateAndNotify(conn);
        synchronized (this.connectionList) {
            this.connectionList.remove(conn);
        }
    }

    @Override
    public void shutdown() {
        if (this.haClient != null) {
            this.haClient.shutdown();
        }
        this.acceptSocketService.shutdown(true);
        this.destroyConnections();
        this.groupTransferService.shutdown();
        this.haConnectionStateNotificationService.shutdown();
    }

    public void destroyConnections() {
        synchronized (this.connectionList) {
            for (HAConnection c : this.connectionList) {
                c.shutdown();
            }

            this.connectionList.clear();
        }
    }

    public DefaultMessageStore getDefaultMessageStore() {
        return defaultMessageStore;
    }

    @Override
    public WaitNotifyObject getWaitNotifyObject() {
        return waitNotifyObject;
    }

    @Override
    public AtomicLong getPush2SlaveMaxOffset() {
        return push2SlaveMaxOffset;
    }

    @Override
    public int inSyncReplicasNums(final long masterPutWhere) {
        int inSyncNums = 1;
        for (HAConnection conn : this.connectionList) {
            if (this.isInSyncSlave(masterPutWhere, conn)) {
                inSyncNums++;
            }
        }
        return inSyncNums;
    }

    protected boolean isInSyncSlave(final long masterPutWhere, HAConnection conn) {
        if (masterPutWhere - conn.getSlaveAckOffset() < this.defaultMessageStore.getMessageStoreConfig()
                .getHaMaxGapNotInSync()) {
            return true;
        }
        return false;
    }

    @Override
    public void putGroupConnectionStateRequest(HAConnectionStateNotificationRequest request) {
        this.haConnectionStateNotificationService.setRequest(request);
    }

    @Override
    public List<HAConnection> getConnectionList() {
        return connectionList;
    }

    @Override
    public HAClient getHAClient() {
        return this.haClient;
    }

    @Override
    public HARuntimeInfo getRuntimeInfo(long masterPutWhere) {
        HARuntimeInfo info = new HARuntimeInfo();

        if (BrokerRole.SLAVE.equals(this.getDefaultMessageStore().getMessageStoreConfig().getBrokerRole())) {
            info.setMaster(false);

            info.getHaClientRuntimeInfo().setMasterAddr(this.haClient.getHaMasterAddress());
            info.getHaClientRuntimeInfo().setMaxOffset(this.getDefaultMessageStore().getMaxPhyOffset());
            info.getHaClientRuntimeInfo().setLastReadTimestamp(this.haClient.getLastReadTimestamp());
            info.getHaClientRuntimeInfo().setLastWriteTimestamp(this.haClient.getLastWriteTimestamp());
            info.getHaClientRuntimeInfo().setTransferredByteInSecond(this.haClient.getTransferredByteInSecond());
            info.getHaClientRuntimeInfo().setMasterFlushOffset(this.defaultMessageStore.getMasterFlushedOffset());
        } else {
            info.setMaster(true);
            int inSyncNums = 0;

            info.setMasterCommitLogMaxOffset(masterPutWhere);

            for (HAConnection conn : this.connectionList) {
                HARuntimeInfo.HAConnectionRuntimeInfo cInfo = new HARuntimeInfo.HAConnectionRuntimeInfo();

                long slaveAckOffset = conn.getSlaveAckOffset();
                cInfo.setSlaveAckOffset(slaveAckOffset);
                cInfo.setDiff(masterPutWhere - slaveAckOffset);
                cInfo.setAddr(conn.getClientAddress().substring(1));
                cInfo.setTransferredByteInSecond(conn.getTransferredByteInSecond());
                cInfo.setTransferFromWhere(conn.getTransferFromWhere());

                boolean isInSync = this.isInSyncSlave(masterPutWhere, conn);
                if (isInSync) {
                    inSyncNums++;
                }
                cInfo.setInSync(isInSync);

                info.getHaConnectionInfo().add(cInfo);
            }
            info.setInSyncSlaveNums(inSyncNums);
        }
        return info;
    }

    /**
     * AcceptSocketService 就是用来监听 slave 的连接，然后创建 HAConnection，默认监听的端口是 listenPort + 1。
     * 在 beginAccept() 中就会基于 NIO 去建立监听通道，注册多路复用器 Selector，来监听 ACCEPT 连接请求。
     * AcceptSocketService 也是一个 ServiceThread，在它的 run 方法中，就是在不断监听slave节点的连接请求，有TCP连接请求过来后，就用连接通道 SocketChannel 创建一个 HAConnection，并启动这个连接，
     * 然后将其添加到 HAService 的连接集合 List<HAConnection> 中
     */
    class DefaultAcceptSocketService extends AcceptSocketService {

        public DefaultAcceptSocketService(final MessageStoreConfig messageStoreConfig) {
            super(messageStoreConfig);
        }

        @Override
        protected HAConnection createConnection(SocketChannel sc) throws IOException {
            return new DefaultHAConnection(DefaultHAService.this, sc);
        }

        @Override
        public String getServiceName() {
            if (defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
                return defaultMessageStore.getBrokerConfig().getIdentifier() + AcceptSocketService.class.getSimpleName();
            }
            return DefaultAcceptSocketService.class.getSimpleName();
        }
    }

    /**
     * AcceptSocketService 就是用来监听 slave 的连接，然后创建 HAConnection，默认监听的端口是 listenPort + 1。
     * 在 beginAccept() 中就会基于 NIO 去建立监听通道，注册多路复用器 Selector，来监听 ACCEPT 连接请求。
     * AcceptSocketService 也是一个 ServiceThread，在它的 run 方法中，就是在不断监听slave节点的连接请求，有TCP连接请求过来后，就用连接通道 SocketChannel 创建一个 HAConnection，并启动这个连接，
     * 然后将其添加到 HAService 的连接集合 List<HAConnection> 中。
     * <p>
     * <p>
     * 监听slave链接创建HAConnection
     * Listens to slave connections to create {@link HAConnection}.
     */
    protected abstract class AcceptSocketService extends ServiceThread {
        private final SocketAddress socketAddressListen;
        private ServerSocketChannel serverSocketChannel;
        private Selector selector;

        private final MessageStoreConfig messageStoreConfig;

        public AcceptSocketService(final MessageStoreConfig messageStoreConfig) {
            this.messageStoreConfig = messageStoreConfig;
            // HA 监听端口 10912
            this.socketAddressListen = new InetSocketAddress(messageStoreConfig.getHaListenPort());
        }

        /**
         * 用于接受Slave连接的AcceptSocketService
         *  AcceptSocketService这个类在Broker的Master和Slaver两个角色启动时都会创建，只不过区别是Slaver开启端口之后，并不会有别的Broker与其建立连接。因为只有在Broker的角色是Slave的时候才会指定要连接的Master地址。
         * 这个逻辑，在Broker启动的时候BrokerController类中运行的。
         * <p>
         * Starts listening to slave connections.
         *
         * @throws Exception If fails.
         */
        public void beginAccept() throws Exception {
            // beginAccept方法就是开启Socket，绑定10912端口，然后注册selector和指定监听的事件为OP_ACCEPT也就是建立连接事件。对应的IO模式为NIO模式。主要看其run方法，这个方法是Master用来接受Slave连接的核心。

            //创建ServerSocketChannel
            // 基于 NIO 建立连接监听
            this.serverSocketChannel = ServerSocketChannel.open();
            //创建selector
            // 打开一个多路复用器
            this.selector = NetworkUtil.openSelector();
            //设置SO_REUSEADDR   https://blog.csdn.net/u010144805/article/details/78579528
            // 设置socket重用地址true
            this.serverSocketChannel.socket().setReuseAddress(true);
            //设置绑定的地址
            // 绑定监听端口
            this.serverSocketChannel.socket().bind(this.socketAddressListen);
            //设置为非阻塞模式
            if (0 == messageStoreConfig.getHaListenPort()) {
                messageStoreConfig.setHaListenPort(this.serverSocketChannel.socket().getLocalPort());
                log.info("OS picked up {} to listen for HA", messageStoreConfig.getHaListenPort());
            }
            this.serverSocketChannel.configureBlocking(false);
            // 注册到多路复用器，监听连接请求
            this.serverSocketChannel.register(this.selector, SelectionKey.OP_ACCEPT);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void shutdown(final boolean interrupt) {
            super.shutdown(interrupt);
            try {
                if (null != this.serverSocketChannel) {
                    this.serverSocketChannel.close();
                }

                if (null != this.selector) {
                    this.selector.close();
                }
            } catch (IOException e) {
                log.error("AcceptSocketService shutdown exception", e);
            }
        }

        /**
         *  这里的逻辑比较简单。就是每过一秒检查一次是否有连接事件，如果有则建立连接，并把建立起来的连接加入到连接列表中进行保存。一直循环这个逻辑。
         * {@inheritDoc}
         */
        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 监听连接到达事件
                    this.selector.select(1000);
                    //获取selector 下的所有selectorKey ，后续迭代用
                    // 连接来了，拿到 SelectionKey
                    Set<SelectionKey> selected = this.selector.selectedKeys();

                    if (selected != null) {
                        for (SelectionKey k : selected) {
                            //检测有连接事件的selectorKey
                            if (k.isAcceptable()) {
                                // 通过 accept 函数完成TCP连接，获取到一个网络连接通道 SocketChannel
                                SocketChannel sc = ((ServerSocketChannel) k.channel()).accept();

                                if (sc != null) {
                                    DefaultHAService.log.info("HAService receive new connection, "
                                            + sc.socket().getRemoteSocketAddress());
                                    try {
                                        //创建HAConnection，建立连接
                                        HAConnection conn = createConnection(sc);
                                        //建立连接
                                        conn.start();
                                        //添加连接到连接列表中
                                        DefaultHAService.this.addConnection(conn);
                                    } catch (Exception e) {
                                        log.error("new HAConnection exception", e);
                                        sc.close();
                                    }
                                }
                            } else {
                                log.warn("Unexpected ops in select " + k.readyOps());
                            }
                        }

                        //清空连接事件，未下一次做准备
                        selected.clear();
                    }
                } catch (Exception e) {
                    log.error(this.getServiceName() + " service has exception.", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        /**
         * Create ha connection
         */
        protected abstract HAConnection createConnection(final SocketChannel sc) throws IOException;
    }
}
