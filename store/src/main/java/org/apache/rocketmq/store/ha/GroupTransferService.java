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

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.CommitLog;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.PutMessageSpinLock;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAConnection;
import org.apache.rocketmq.store.ha.autoswitch.AutoSwitchHAService;

/**
 * 检查同步进度和唤醒CommitLog刷盘线程的GroupTransferService
 *  GroupTransferService是CommitLog消息刷盘的类CommitLog与HAService打交道的一个中间类。在CommitLog中进行主从刷盘的时候，会创建一个CommitLog.GroupCommitRequest的内部类，这个类包含了当前Broker最新的消息的物理偏移量信息。然后把这个类丢给GroupTransferService处理，然后唤醒GroupTransferService。起始这个逻辑跟CommitLog内部的GroupCommitService逻辑一样。只不过对于同步部分的逻辑不一样
 * ————————————————
 * 版权声明：本文为CSDN博主「szhlcy」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
 * 原文链接：https://blog.csdn.net/szhlcy/article/details/116055627
 * <p>
 * GroupTransferService Service
 */
public class GroupTransferService extends ServiceThread {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    private final WaitNotifyObject notifyTransferObject = new WaitNotifyObject();
    private final PutMessageSpinLock lock = new PutMessageSpinLock();
    private final DefaultMessageStore defaultMessageStore;
    private final HAService haService;
    private volatile List<CommitLog.GroupCommitRequest> requestsWrite = new LinkedList<>();
    private volatile List<CommitLog.GroupCommitRequest> requestsRead = new LinkedList<>();

    public GroupTransferService(final HAService haService, final DefaultMessageStore defaultMessageStore) {
        this.haService = haService;
        this.defaultMessageStore = defaultMessageStore;
    }

    public void putRequest(final CommitLog.GroupCommitRequest request) {
        lock.lock();
        try {
            this.requestsWrite.add(request);
        } finally {
            lock.unlock();
        }
        wakeup();
    }

    public void notifyTransferSome() {
        this.notifyTransferObject.wakeup();
    }

    private void swapRequests() {
        lock.lock();
        try {
            List<CommitLog.GroupCommitRequest> tmp = this.requestsWrite;
            this.requestsWrite = this.requestsRead;
            this.requestsRead = tmp;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 比较Master推送到Slave的 偏移量push2SlaveMaxOffset是不是大于传进来的CommitLog.GroupCommitRequest中的偏移量
     * 计算本次同步超时的时间节点，时间为当前时间加上参数系统配置参数syncFlushTimeout默认为5秒
     * 如果第一步结果为true，则返回结果为PUT_OK。如果第一步为false，则每过一秒检查一次结果，如果超过5次了还没同步完成，则表示超时了那么返回结果为FLUSH_SLAVE_TIMEOUT。同时会唤醒CommitLog的刷盘线程。
     * ————————————————
     * 版权声明：本文为CSDN博主「szhlcy」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
     * 原文链接：https://blog.csdn.net/szhlcy/article/details/116055627
     */
    private void doWaitTransfer() {
        //如果读请求不为空
        if (!this.requestsRead.isEmpty()) {
            for (CommitLog.GroupCommitRequest req : this.requestsRead) {
                boolean transferOK = false;

                long deadLine = req.getDeadLine();
                final boolean allAckInSyncStateSet = req.getAckNums() == MixAll.ALL_ACK_IN_SYNC_STATE_SET;

                for (int i = 0; !transferOK && deadLine - System.nanoTime() > 0; i++) {
                    if (i > 0) {
                        this.notifyTransferObject.waitForRunning(1);
                    }

                    if (!allAckInSyncStateSet && req.getAckNums() <= 1) {
                        //如果push到slave的偏移量 大于等于 请求中的消息的最大偏移量 表示slave同步完成
                        transferOK = haService.getPush2SlaveMaxOffset().get() >= req.getNextOffset();
                        continue;
                    }

                    if (allAckInSyncStateSet && this.haService instanceof AutoSwitchHAService) {
                        // In this mode, we must wait for all replicas that in SyncStateSet.
                        final AutoSwitchHAService autoSwitchHAService = (AutoSwitchHAService) this.haService;
                        final Set<Long> syncStateSet = autoSwitchHAService.getSyncStateSet();
                        if (syncStateSet.size() <= 1) {
                            // Only master
                            transferOK = true;
                            break;
                        }

                        // Include master
                        int ackNums = 1;
                        for (HAConnection conn : haService.getConnectionList()) {
                            final AutoSwitchHAConnection autoSwitchHAConnection = (AutoSwitchHAConnection) conn;
                            if (syncStateSet.contains(autoSwitchHAConnection.getSlaveId()) && autoSwitchHAConnection.getSlaveAckOffset() >= req.getNextOffset()) {
                                ackNums++;
                            }
                            if (ackNums >= syncStateSet.size()) {
                                transferOK = true;
                                break;
                            }
                        }
                    } else {
                        // Include master
                        int ackNums = 1;
                        for (HAConnection conn : haService.getConnectionList()) {
                            // TODO: We must ensure every HAConnection represents a different slave
                            // Solution: Consider assign a unique and fixed IP:ADDR for each different slave
                            if (conn.getSlaveAckOffset() >= req.getNextOffset()) {
                                ackNums++;
                            }
                            if (ackNums >= req.getAckNums()) {
                                transferOK = true;
                                break;
                            }
                        }
                    }
                }

                if (!transferOK) {
                    log.warn("transfer message to slave timeout, offset : {}, request acks: {}",
                            req.getNextOffset(), req.getAckNums());
                }

                req.wakeupCustomer(transferOK ? PutMessageStatus.PUT_OK : PutMessageStatus.FLUSH_SLAVE_TIMEOUT);
            }

            this.requestsRead = new LinkedList<>();
        }
    }

    // 在run方法中会将传入的CommitLog.GroupCommitRequest从requestsWrite转换到requestsRead中然后进行处理检查对应的同步请求的进度。检查的逻辑在doWaitTransfer中
    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                /**
                 * 这里进入等待，等待被唤醒，进入等待之前会调用onWaitEnd方法，然后调用swapRequests方法，
                 * 吧requestsWrite转换为requestsRead
                 */
                this.waitForRunning(10);
                /**
                 * 进行请求处理
                 */
                this.doWaitTransfer();
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    protected void onWaitEnd() {
        this.swapRequests();
    }

    @Override
    public String getServiceName() {
        if (defaultMessageStore != null && defaultMessageStore.getBrokerConfig().isInBrokerContainer()) {
            return defaultMessageStore.getBrokerIdentity().getIdentifier() + GroupTransferService.class.getSimpleName();
        }
        return GroupTransferService.class.getSimpleName();
    }
}
