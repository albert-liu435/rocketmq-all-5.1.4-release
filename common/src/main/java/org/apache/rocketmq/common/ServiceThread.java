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
package org.apache.rocketmq.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;

/**
 * 线程服务类
 * 提供了可以优雅地终止线程的机制，并实现了等待机制。
 */
public abstract class ServiceThread implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private static final long JOIN_TIME = 90 * 1000;

    protected Thread thread;
    // waitPoint 起到主线程通知子线程的作用
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
    // 是通知标识
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
    //服务类是否停止
    protected volatile boolean stopped = false;
    // 是否守护线程
    protected boolean isDaemon = false;

    //Make it able to restart the thread
    // 线程开始标识
    private final AtomicBoolean started = new AtomicBoolean(false);

    public ServiceThread() {

    }

    // 获取线程名称
    public abstract String getServiceName();

    /**
     * 开始运行：
     * <p>
     * 在调用 start() 开始执行任务时，首先设置 started 标识，标记任务已经开始。
     * 接着设置了 stopped 标识，run() 方法里可以通过 isStopped() 来判断是否继续执行。
     * 然后绑定一个执行的 Thread，并启动这个线程开始运行。
     */
    // 开始执行任务
    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        // 任务已经开始运行标识
        if (!started.compareAndSet(false, true)) {
            return;
        }
        // 停止标识设置为 false
        stopped = false;
        // 绑定线程，运行当前任务
        this.thread = new Thread(this, getServiceName());
        // 设置守护线程，守护线程具有最低的优先级，一般用于为系统中的其它对象和线程提供服务
        this.thread.setDaemon(isDaemon);
        // 启动线程开始运行
        this.thread.start();
        log.info("Start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
    }

    public void shutdown() {
        this.shutdown(false);
    }

    /**
     * 主线程可调用 shutdown() 方法来终止 run() 方法的运行
     * 其终止的方式就是设置 stopped 标识，这样 run() 方法就可以通过 isStopped() 来跳出 while 循环
     * 然后 waitPoint 计数器减 1（减为0），这样做的目的就是如果线程调用了 waitForRunning 方法正在等待中，这样可以通知它不要等待了。ServiceThread 巧妙的使用了 CountDownLatch 来实现了等待，以及终止时的通知唤醒机制。
     * 最后调用 t.join() 方法等待 run() 方法执行完成。
     * <p>
     *
     * @param interrupt
     */
    public void shutdown(final boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        // 任务必须已经开始
        if (!started.compareAndSet(true, false)) {
            return;
        }
        // 设置停止标识
        this.stopped = true;
        log.info("shutdown thread[{}] interrupt={} ", getServiceName(), interrupt);

        //if thead is waiting, wakeup it
        wakeup();

        try {
            // 中断线程，设置中断标识
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            // 守护线程等待执行完毕
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJoinTime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread[{}], elapsed time: {}ms, join time:{}ms", getServiceName(), elapsedTime, this.getJoinTime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJoinTime() {
        return JOIN_TIME;
    }

    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread[{}] ", this.getServiceName());
    }

    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            // 计数减1，通知等待的线程不要等待了
            waitPoint.countDown(); // notify
        }
    }

    /**
     * 等待运行
     * <p>
     * 子类可调用 waitForRunning() 方法等待指定时间后再运行
     * 如果 hasNotified 已经通知过，就不等待
     * 否则重置 waitPoint 计数器（默认为 1）
     * 然后调用 waitPoint.await 开始等待，它会等待直到超时或者计数器减为 0
     *
     * @param interval 等待间隔
     */
    // 等待一定时间后运行
    protected void waitForRunning(long interval) {
        //
        if (hasNotified.compareAndSet(true, false)) {
            // 通知等待结束
            this.onWaitEnd();
            return;
        }

        //entry to wait
        //进入等待
        // 重置计数
        waitPoint.reset();

        try {
            // 一直等待，直到计数减为 0，或者超时
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            // 设置未通知
            hasNotified.set(false);
            // 通知等待结束
            this.onWaitEnd();
        }
    }

    protected void onWaitEnd() {
    }

    public boolean isStopped() {
        return stopped;
    }

    public boolean isDaemon() {
        return isDaemon;
    }

    public void setDaemon(boolean daemon) {
        isDaemon = daemon;
    }
}
