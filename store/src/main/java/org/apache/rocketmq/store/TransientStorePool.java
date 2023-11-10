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

import com.sun.jna.NativeLong;
import com.sun.jna.Pointer;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.store.util.LibC;
import sun.nio.ch.DirectBuffer;

/**
 * 来看一下瞬时存储池TransientStorePool的设计。它在创建时池子大小默认是 5，文件大小默认为 commitlog 文件大小（1GB），也就是说这是专门针对 commitlog 的池子。然后用一个Deque双端队列来存储预分配的ByteBuffer对象，
 * 这个瞬时存储池的对象就是 ByteBuffer。
 */
public class TransientStorePool {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //池中预分配ByteBuffer数量
    private final int poolSize;
    //每个ByteBuffer大小
    private final int fileSize;
    //采用双端队列维护预分配的ByteBuffer
    private final Deque<ByteBuffer> availableBuffers;
    private volatile boolean isRealCommit = true;

    public TransientStorePool(final int poolSize, final int fileSize) {
        this.poolSize = poolSize;
        this.fileSize = fileSize;
        this.availableBuffers = new ConcurrentLinkedDeque<>();
    }

    /**
     * 因为这里需要申请多个堆外ByteBuffer，所以是个十分heavy的初始化方法
     * It's a heavy init method.
     */
    public void init() {
        for (int i = 0; i < poolSize; i++) {
            //申请直接内存空间
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(fileSize);

            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            //锁住内存，避免操作系统虚拟内存的换入换出
            LibC.INSTANCE.mlock(pointer, new NativeLong(fileSize));
            //将预分配的ByteBuffer方法队列中
            availableBuffers.offer(byteBuffer);
        }
    }

    /**
     * 销毁内存池
     */
    public void destroy() {
//        然后它的初始化方法 init() 中，会循环分配出5块 ByteBuffer，最后将这块ByteBuffer放入队列中，等待使用。
        //取消对内存的锁定
        for (ByteBuffer byteBuffer : availableBuffers) {
            final long address = ((DirectBuffer) byteBuffer).address();
            Pointer pointer = new Pointer(address);
            LibC.INSTANCE.munlock(pointer, new NativeLong(fileSize));
        }
    }

    /**
     * 使用完毕之后归还ByteBuffer
     *
     * @param byteBuffer
     */
    public void returnBuffer(ByteBuffer byteBuffer) {
        //ByteBuffer各下标复位
        byteBuffer.position(0);
        byteBuffer.limit(fileSize);
        //放入队头，等待下次重新被分配
        this.availableBuffers.offerFirst(byteBuffer);
    }

    /**
     * 从池中获取ByteBuffer
     *
     * @return
     */
    public ByteBuffer borrowBuffer() {
        //非阻塞弹出队头元素，如果没有启用暂存池，则
        //不会调用init方法，队列中就没有元素，这里返回null
        //其次，如果队列中所有元素都被借用出去，队列也为空
        //此时也会返回null
        ByteBuffer buffer = availableBuffers.pollFirst();
        //如果队列中剩余元素数量小于配置个数的0.4，则写日志提示
        if (availableBuffers.size() < poolSize * 0.4) {
            log.warn("TransientStorePool only remain {} sheets.", availableBuffers.size());
        }
        return buffer;
    }


    public int availableBufferNums() {
        return availableBuffers.size();
    }

    public boolean isRealCommit() {
        return isRealCommit;
    }

    public void setRealCommit(boolean realCommit) {
        isRealCommit = realCommit;
    }
}
