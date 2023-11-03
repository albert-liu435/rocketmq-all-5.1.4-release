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
 * https://blog.csdn.net/weixin_43248318/article/details/124032499
 * https://zhuanlan.zhihu.com/p/513814016
 * <p>
 * <p>
 * <p>
 * <p>
 * 在什么场景下要开启TransientStorePool（技术为解决问题而出现）
 * 在 RocketMQ 中，TransientStorePool 是一种优化磁盘 I/O 性能的机制。它通过预分配内存块，将消息写入预分配的内存块（直接内存），然后使用内存映射文件（Memory-Mapped File）将内存块中的数据刷到磁盘，从而提高写入性能。因此，当你需要提高 RocketMQ 的消息写入性能时，可以考虑开启 TransientStorePool。
 * ————————————————
 * 版权声明：本文为CSDN博主「翁正存」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
 * 原文链接：https://blog.csdn.net/Wengzhengcun/article/details/131239995
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
