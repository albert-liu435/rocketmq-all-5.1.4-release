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
package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.opentelemetry.api.common.AttributesBuilder;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nullable;

import org.apache.rocketmq.common.AbortProcessException;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.ChannelEventListener;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.metrics.RemotingMetricsManager;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;

import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_IS_LONG_POLLING;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_REQUEST_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESPONSE_CODE;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.LABEL_RESULT;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_ONEWAY;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_PROCESS_REQUEST_FAILED;
import static org.apache.rocketmq.remoting.metrics.RemotingMetricsConstant.RESULT_WRITE_CHANNEL_FAILED;

/**
 * RocketMQ 抽象了一个 Netty 远程通信抽象基类 NettyRemotingAbstract，它对 Netty 服务器程序进行了封装，负责请求和响应的处理分发、执行，下面就先来看看这个基类的功能
 */
public abstract class NettyRemotingAbstract {

    /**
     * Remoting logger instance.
     */
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    /**
     * Oneway 请求类型的信号量，用来限制 Oneway 请求类型的并发度
     * Semaphore to limit maximum number of on-going one-way requests, which protects system memory footprint.
     */
    protected final Semaphore semaphoreOneway;

    /**
     * Async 请求类型的信号量，用来限制 Async 请求类型的并发度
     * Semaphore to limit maximum number of on-going asynchronous requests, which protects system memory footprint.
     */
    protected final Semaphore semaphoreAsync;

    /**
     * 发起请求之后，将等待请求以及回调等操作封装到 ResponseFuture 中，在响应回来之后可以接着进行后续操作。responseTable 就存储了请求编码与 ResponseFuture 的关系。
     * This map caches all on-going requests.
     */
    protected final ConcurrentMap<Integer /* opaque */, ResponseFuture> responseTable =
            new ConcurrentHashMap<>(256);

    /**
     * 这个表存储了请求编码与之对应的处理器和线程池
     * This container holds all processors per request code, aka, for each incoming request, we may look up the
     * responding processor in this map to handle the request.
     */
    protected final HashMap<Integer/* request code */, Pair<NettyRequestProcessor, ExecutorService>> processorTable =
            new HashMap<>(64);

    /**
     * Netty 事件执行器
     * Executor to feed netty events to user defined {@link ChannelEventListener}.
     */
    protected final NettyEventExecutor nettyEventExecutor = new NettyEventExecutor();

    /**
     * 请求默认处理器，如果 processorTable 没有特定的处理器，就使用这个默认处理器。
     * The default request processor to use in case there is no exact match in {@link #processorTable} per request
     * code.
     */
    protected Pair<NettyRequestProcessor, ExecutorService> defaultRequestProcessorPair;

    /**
     * SSL 上下文。
     * SSL context via which to create {@link SslHandler}.
     */
    protected volatile SslContext sslContext;

    /**
     * RPC 回调钩子函数。
     * custom rpc hooks
     */
    protected List<RPCHook> rpcHooks = new ArrayList<>();

    static {
        NettyLogger.initNettyLogger();
    }

    /**
     * Constructor, specifying capacity of one-way and asynchronous semaphores.
     *
     * @param permitsOneway Number of permits for one-way requests.
     * @param permitsAsync  Number of permits for asynchronous requests.
     */
    public NettyRemotingAbstract(final int permitsOneway, final int permitsAsync) {
        // 创建两个信号量
        this.semaphoreOneway = new Semaphore(permitsOneway, true);
        this.semaphoreAsync = new Semaphore(permitsAsync, true);
    }

    /**
     * 抽象方法：获取网络连接事件监听器
     * Custom channel event listener.
     *
     * @return custom channel event listener if defined; null otherwise.
     */
    public abstract ChannelEventListener getChannelEventListener();

    /**
     * 向执行器中如一个netty事件
     * Put a netty event to the executor.
     *
     * @param event Netty event instance.
     */
    public void putNettyEvent(final NettyEvent event) {
        this.nettyEventExecutor.putNettyEvent(event);
    }

    /**
     * 处理消息请求和响应
     * Entry of incoming command processing.
     *
     * <p>
     * <strong>Note:</strong>
     * The incoming remoting command may be
     * <ul>
     * <li>An inquiry request from a remote peer component;</li>
     * <li>A response to a previous request issued by this very participant.</li>
     * </ul>
     * </p>
     *
     * @param ctx Channel handler context. 就是 Netty 中的连接上下文，通过它可以拿到当前的 Channel、触发管道读写事件、写回响应数据等。
     * @param msg incoming remoting command. 请求数据的封装，是 RocketMQ 中的通信协议。其在 Netty 网络通道中传输时，会进行序列化、反序列化以及编解码。
     */
    public void processMessageReceived(ChannelHandlerContext ctx, RemotingCommand msg) {
        if (msg != null) {
            switch (msg.getType()) {
                //处理请求
                case REQUEST_COMMAND:
                    // 处理发送命令
                    processRequestCommand(ctx, msg);
                    break;
                //处理响应
                case RESPONSE_COMMAND:
                    processResponseCommand(ctx, msg);
                    break;
                default:
                    break;
            }
        }
    }

    protected void doBeforeRpcHooks(String addr, RemotingCommand request) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook : rpcHooks) {
                rpcHook.doBeforeRequest(addr, request);
            }
        }
    }

    public void doAfterRpcHooks(String addr, RemotingCommand request, RemotingCommand response) {
        if (rpcHooks.size() > 0) {
            for (RPCHook rpcHook : rpcHooks) {
                rpcHook.doAfterResponse(addr, request, response);
            }
        }
    }

    public static void writeResponse(Channel channel, RemotingCommand request, @Nullable RemotingCommand response) {
        writeResponse(channel, request, response, null);
    }

    public static void writeResponse(Channel channel, RemotingCommand request, @Nullable RemotingCommand response,
                                     Consumer<Future<?>> callback) {
        if (response == null) {
            return;
        }
        AttributesBuilder attributesBuilder = RemotingMetricsManager.newAttributesBuilder()
                .put(LABEL_IS_LONG_POLLING, request.isSuspended())
                .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(request.getCode()))
                .put(LABEL_RESPONSE_CODE, RemotingHelper.getResponseCodeDesc(response.getCode()));
        if (request.isOnewayRPC()) {
            attributesBuilder.put(LABEL_RESULT, RESULT_ONEWAY);
            RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
            return;
        }
        response.setOpaque(request.getOpaque());
        response.markResponseType();
        try {
            channel.writeAndFlush(response).addListener((ChannelFutureListener) future -> {
                if (future.isSuccess()) {
                    log.debug("Response[request code: {}, response code: {}, opaque: {}] is written to channel{}",
                            request.getCode(), response.getCode(), response.getOpaque(), channel);
                } else {
                    log.error("Failed to write response[request code: {}, response code: {}, opaque: {}] to channel{}",
                            request.getCode(), response.getCode(), response.getOpaque(), channel, future.cause());
                }
                attributesBuilder.put(LABEL_RESULT, RemotingMetricsManager.getWriteAndFlushResult(future));
                RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
                if (callback != null) {
                    callback.accept(future);
                }
            });
        } catch (Throwable e) {
            log.error("process request over, but response failed", e);
            log.error(request.toString());
            log.error(response.toString());
            attributesBuilder.put(LABEL_RESULT, RESULT_WRITE_CHANNEL_FAILED);
            RemotingMetricsManager.rpcLatency.record(request.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
        }
    }

    /**
     * 根据请求命令从processorTable中找到对应的NettyRequestProcessor，封装成RequestTask，提交给对应的messageExecutor来执行。
     * <p>
     * 从处理器表processorTable获取请求编码对应的处理器和线程池，没有则使用默认的处理器和线程池
     * 接着将请求和响应的处理封装成一个 Runnable
     * 判断是否拒绝请求，是的就返回系统繁忙的编码 SYSTEM_BUSY，将响应写入 Channel。从这可以看出，将数据响应回客户端是通过 ctx.writeAndFlush() 来完成
     * 然后将前面封装的 Runnable，以及 ctx、cmd 封装成一个 RequestTask
     * 最后将 RequestTask 提交到线程池里去执行，如果线程池满了被拒绝请求，则响应系统繁忙。
     * <p>
     * <p>
     * <p>
     * <p>
     * Process incoming request command issued by remote peer.
     *
     * @param ctx channel handler context.
     * @param cmd request command.
     */
    public void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand cmd) {
        // processorTable是在BrokerController初始化的时候注册的(具体在BrokerController.initialize -> BrokerController.registerProcessor)
        // 支持不同的请求设置不同的处理器和线程池
        final Pair<NettyRequestProcessor, ExecutorService> matched = this.processorTable.get(cmd.getCode());
        // 没有特定的处理器则使用默认的处理器
        final Pair<NettyRequestProcessor, ExecutorService> pair = null == matched ? this.defaultRequestProcessorPair : matched;
        // 请求ID
        final int opaque = cmd.getOpaque();

        //为null的话则直接返回
        if (pair == null) {
            String error = " request type " + cmd.getCode() + " not supported";
            final RemotingCommand response =
                    RemotingCommand.createResponseCommand(RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED, error);
            response.setOpaque(opaque);
            writeResponse(ctx.channel(), cmd, response);
            log.error(RemotingHelper.parseChannelRemoteAddr(ctx.channel()) + error);
            return;
        }
        // 封装 Runnable
        Runnable run = buildProcessRequestHandler(ctx, cmd, pair, opaque);

        // 是否拒绝请求
        if (pair.getObject1().rejectRequest()) {
            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                    "[REJECTREQUEST]system busy, start flow control for a while");
            response.setOpaque(opaque);
            writeResponse(ctx.channel(), cmd, response);
            return;
        }

        try {
            // 封装一个请求任务
            final RequestTask requestTask = new RequestTask(run, ctx.channel(), cmd);
            //async execute task, current thread return directly
            // 提交到线程池异步执行
            pair.getObject2().submit(requestTask);
        } catch (RejectedExecutionException e) {
            if ((System.currentTimeMillis() % 10000) == 0) {
                log.warn(RemotingHelper.parseChannelRemoteAddr(ctx.channel())
                        + ", too many requests and system thread pool busy, RejectedExecutionException "
                        + pair.getObject2().toString()
                        + " request code: " + cmd.getCode());
            }

            final RemotingCommand response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_BUSY,
                    "[OVERLOAD]system busy, start flow control for a while");
            response.setOpaque(opaque);
            writeResponse(ctx.channel(), cmd, response);
        } catch (Throwable e) {
            AttributesBuilder attributesBuilder = RemotingMetricsManager.newAttributesBuilder()
                    .put(LABEL_REQUEST_CODE, RemotingHelper.getRequestCodeDesc(cmd.getCode()))
                    .put(LABEL_RESULT, RESULT_PROCESS_REQUEST_FAILED);
            RemotingMetricsManager.rpcLatency.record(cmd.getProcessTimer().elapsed(TimeUnit.MILLISECONDS), attributesBuilder.build());
        }
    }

    /**
     * 请求处理的主要逻辑封装在 Runnable 中，主要逻辑如下：
     * <p>
     * 读取 Channel 远程通信地址
     * 执行前置 RPC 钩子函数
     * 封装一个响应回调 RemotingResponseCallback，在处理完请求后将调用这个回调。这个回调会先执行后置 RPC 钩子函数，然后对非 Oneway 类型请求，将响应通过 Channel 写回调用者。
     * 最后使用处理器来处理请求，如果是异步处理器则异步处理请求；如果是同步处理器，则同步处理请求，然后同步执行回调。
     * <p>
     * 从这里可以看出，请求执行完成后响应客户端是在 RemotingResponseCallback 回调中完成的，对于 Oneway 类型的请求，由于只执行请求不关心响应，因此不会响应调用者。
     * 而在请求前或请求完成后，想要对请求做一些定制化可以注册自定义 RPCHook 钩子函数来完成。
     *
     * @param ctx
     * @param cmd
     * @param pair
     * @param opaque
     * @return
     */
    private Runnable buildProcessRequestHandler(ChannelHandlerContext ctx, RemotingCommand cmd,
                                                Pair<NettyRequestProcessor, ExecutorService> pair, int opaque) {
        return () -> {
            Exception exception = null;
            RemotingCommand response;

            try {
                // 获取通道远程Broker地址
                String remoteAddr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                try {
                    // 请求之前执行RPC钩子
                    doBeforeRpcHooks(remoteAddr, cmd);
                } catch (AbortProcessException e) {
                    throw e;
                } catch (Exception e) {
                    exception = e;
                }

                if (exception == null) {
                    // 处理请求
//                    核心逻辑在pair.getObject1()的processRequest()方法中。那么pair.getObject1()取到的是什么呢，pair是由请求命令的code为key从processorTable中获取的，而processorTable是由上一篇文章RocketMQ笔记(十四)Broker源码分析-启动流程中介绍的BrokerController的初始化initialize方法中调用registerProcessor()进行注册的，对于发送消息，此处注册的是SendMessageProcessor，注册主要部分源码如下

                    response = pair.getObject1().processRequest(ctx, cmd);
                } else {
                    response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, null);
                }

                try {
                    doAfterRpcHooks(remoteAddr, cmd, response);
                } catch (AbortProcessException e) {
                    throw e;
                } catch (Exception e) {
                    exception = e;
                }

                if (exception != null) {
                    throw exception;
                }

                writeResponse(ctx.channel(), cmd, response);
            } catch (AbortProcessException e) {
                response = RemotingCommand.createResponseCommand(e.getResponseCode(), e.getErrorMessage());
                response.setOpaque(opaque);
                writeResponse(ctx.channel(), cmd, response);
            } catch (Throwable e) {
                log.error("process request exception", e);
                log.error(cmd.toString());

                if (!cmd.isOnewayRPC()) {
                    response = RemotingCommand.createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                            UtilAll.exceptionSimpleDesc(e));
                    response.setOpaque(opaque);
                    writeResponse(ctx.channel(), cmd, response);
                }
            }
        };
    }

    /**
     * 在接收服务端的响应后，将由 processResponseCommand 来处理响应，主要流程如下：
     * <p>
     * 从响应表 responseTable 中获取对应 ResponseFuture，这是在执行请求时存起来的，然后将其从 responseTable 表中移除。
     * 如果是异步调用，一般会设置一个执行回调，这时就会去执行回调函数
     * 如果是同步调用，就会调用 putResponse 通知等待方响应回来了。
     * <p>
     * <p>
     * Process response from remote peer to the previous issued requests.
     *
     * @param ctx channel handler context.
     * @param cmd response command instance.
     */
    public void processResponseCommand(ChannelHandlerContext ctx, RemotingCommand cmd) {
        // 请求ID
        final int opaque = cmd.getOpaque();
        // 获取响应 Future
        final ResponseFuture responseFuture = responseTable.get(opaque);
        if (responseFuture != null) {
            responseFuture.setResponseCommand(cmd);
            // 移除
            responseTable.remove(opaque);

            // 异步调用
            if (responseFuture.getInvokeCallback() != null) {
                executeInvokeCallback(responseFuture);
            } else {
                // 同步调用
                responseFuture.putResponse(cmd);
                responseFuture.release();
            }
        } else {
            log.warn("receive response, but not matched any request, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            log.warn(cmd.toString());
        }
    }

    /**
     * Execute callback in callback executor. If callback executor is null, run directly in current thread
     */
    private void executeInvokeCallback(final ResponseFuture responseFuture) {
        boolean runInThisThread = false;
        ExecutorService executor = this.getCallbackExecutor();
        if (executor != null && !executor.isShutdown()) {
            try {
                executor.submit(() -> {
                    try {
                        responseFuture.executeInvokeCallback();
                    } catch (Throwable e) {
                        log.warn("execute callback in executor exception, and callback throw", e);
                    } finally {
                        responseFuture.release();
                    }
                });
            } catch (Exception e) {
                runInThisThread = true;
                log.warn("execute callback in executor exception, maybe executor busy", e);
            }
        } else {
            runInThisThread = true;
        }

        if (runInThisThread) {
            try {
                responseFuture.executeInvokeCallback();
            } catch (Throwable e) {
                log.warn("executeInvokeCallback Exception", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * Custom RPC hooks.
     *
     * @return RPC hooks if specified; null otherwise.
     */
    public List<RPCHook> getRPCHook() {
        return rpcHooks;
    }

    public void registerRPCHook(RPCHook rpcHook) {
        if (rpcHook != null && !rpcHooks.contains(rpcHook)) {
            rpcHooks.add(rpcHook);
        }
    }

    public void clearRPCHook() {
        rpcHooks.clear();
    }

    /**
     * 抽象方法：获取回调的线程池
     * This method specifies thread pool to use while invoking callback methods.
     *
     * @return Dedicated thread pool instance if specified; or null if the callback is supposed to be executed in the
     * netty event-loop thread.
     */
    public abstract ExecutorService getCallbackExecutor();

    /**
     * 就是扫描 responseTable 响应表，判断如果请求超时了，就将其从响应表移除，并释放占用的资源（信号量），并且对于超时的请求，会直接执行回调。
     * 其实我们就是要注意对于内存表中的数据，一定要有过期机制，比如这里采用的定时任务扫描过期的数据，然后从内存表中移除，避免内存表越来越大而发生OOM的情况。
     * <p>
     * This method is periodically invoked to scan and expire deprecated request.
     * </p>
     */
    public void scanResponseTable() {
        final List<ResponseFuture> rfList = new LinkedList<>();
        Iterator<Entry<Integer, ResponseFuture>> it = this.responseTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, ResponseFuture> next = it.next();
            ResponseFuture rep = next.getValue();

            if ((rep.getBeginTimestamp() + rep.getTimeoutMillis() + 1000) <= System.currentTimeMillis()) {
                rep.release();
                it.remove();
                rfList.add(rep);
                log.warn("remove timeout request, " + rep);
            }
        }
        // 超时的请求直接执行回调
        for (ResponseFuture rf : rfList) {
            try {
                executeInvokeCallback(rf);
            } catch (Throwable e) {
                log.warn("scanResponseTable, operationComplete Exception", e);
            }
        }
    }

    /**
     * 将当前请求ID、Channel 封装一个 ResponseFuture，然后放入响应表 responseTable 中。
     * 将请求数据写入 Channel，并添加一个 Channel 监听器，在请求完成后设置 ResponseFuture 的状态
     * 然后调用 responseFuture.waitResponse() 开始等待响应，直到响应回来或者超时，最后从 responseTable 表中移除当前请求
     * <p>
     * 同步执行就是在主线程中同步调用服务端接口，然后同步等待响应结果直到超时。
     *
     * @param channel
     * @param request
     * @param timeoutMillis
     * @return
     * @throws InterruptedException
     * @throws RemotingSendRequestException
     * @throws RemotingTimeoutException
     */
    public RemotingCommand invokeSyncImpl(final Channel channel, final RemotingCommand request,
                                          final long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException {
        //get the request id
        // 请求ID
        final int opaque = request.getOpaque();

        try {
            // 封装响应 Future
            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis, null, null);
            // 暂存到响应表
            this.responseTable.put(opaque, responseFuture);
            final SocketAddress addr = channel.remoteAddress();
            // 把数据写回客户端，并添加一个监听器
            channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
                // 请求完成后的事件监听
                if (f.isSuccess()) {
                    // 请求成功
                    responseFuture.setSendRequestOK(true);
                    return;
                }
                // 请求失败
                responseFuture.setSendRequestOK(false);
                responseTable.remove(opaque);
                responseFuture.setCause(f.cause());
                responseFuture.putResponse(null);
                log.warn("Failed to write a request command to {}, caused by underlying I/O operation failure", addr);
            });

            // 同步等待响应直到完成或超时
            RemotingCommand responseCommand = responseFuture.waitResponse(timeoutMillis);
            if (null == responseCommand) {
                // 请求超时
                if (responseFuture.isSendRequestOK()) {
                    throw new RemotingTimeoutException(RemotingHelper.parseSocketAddressAddr(addr), timeoutMillis,
                            responseFuture.getCause());
                } else {
                    // 请求发送异常
                    throw new RemotingSendRequestException(RemotingHelper.parseSocketAddressAddr(addr), responseFuture.getCause());
                }
            }

            return responseCommand;
        } finally {
            // 移除 ResponseFuture
            this.responseTable.remove(opaque);
        }
    }

    /**
     * 异步执行的流程如下：
     * <p>
     * 先通过异步信号量semaphoreAsync获取到一个许可证，如果并发太高获取不到信号量，就可能超时
     * 拿到信号量许可证后，封装一个信号量释放器 SemaphoreReleaseOnlyOnce，它主要的作用是后面释放这个许可证
     * 接着将 Channel、请求ID、执行回调、信号量释放器等封装到 ResponseFuture 中，然后放到响应表 responseTable 中。
     * 之后就是数据发送到服务端，然后注册一个监听器，在请求完成后设置请求状态。
     * 请求完成后，最后释放资源，其实就是在释放信号量的许可证。
     * <p>
     * 异步执行会通过一个信号量来控制异步执行的并发度（默认64），异步执行有回调，它被封装到 ResponseFuture，响应回来后会进入 processResponseCommand 中处理，
     * 在里面就会调用 responseFuture.executeInvokeCallback() 来执行这个回调。
     *
     * @param channel
     * @param request
     * @param timeoutMillis
     * @param invokeCallback
     * @throws InterruptedException
     * @throws RemotingTooMuchRequestException
     * @throws RemotingTimeoutException
     * @throws RemotingSendRequestException
     */
    public void invokeAsyncImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis,
                                final InvokeCallback invokeCallback)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        long beginStartTime = System.currentTimeMillis();
        final int opaque = request.getOpaque();
        boolean acquired = this.semaphoreAsync.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreAsync);
            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeoutMillis < costTime) {
                once.release();
                throw new RemotingTimeoutException("invokeAsyncImpl call timeout");
            }

            final ResponseFuture responseFuture = new ResponseFuture(channel, opaque, timeoutMillis - costTime, invokeCallback, once);
            this.responseTable.put(opaque, responseFuture);
            try {
                channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
                    if (f.isSuccess()) {
                        responseFuture.setSendRequestOK(true);
                        return;
                    }
                    requestFail(opaque);
                    log.warn("send a request command to channel <{}> failed.", RemotingHelper.parseChannelRemoteAddr(channel));
                });
            } catch (Exception e) {
                responseTable.remove(opaque);
                responseFuture.release();
                log.warn("send a request command to channel <" + RemotingHelper.parseChannelRemoteAddr(channel) + "> Exception", e);
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeAsyncImpl invoke too fast");
            } else {
                String info =
                        String.format("invokeAsyncImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreAsyncValue: %d",
                                timeoutMillis,
                                this.semaphoreAsync.getQueueLength(),
                                this.semaphoreAsync.availablePermits()
                        );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    private void requestFail(final int opaque) {
        ResponseFuture responseFuture = responseTable.remove(opaque);
        if (responseFuture != null) {
            responseFuture.setSendRequestOK(false);
            responseFuture.putResponse(null);
            try {
                executeInvokeCallback(responseFuture);
            } catch (Throwable e) {
                log.warn("execute callback in requestFail, and callback throw", e);
            } finally {
                responseFuture.release();
            }
        }
    }

    /**
     * mark the request of the specified channel as fail and to invoke fail callback immediately
     *
     * @param channel the channel which is close already
     */
    protected void failFast(final Channel channel) {
        for (Entry<Integer, ResponseFuture> entry : responseTable.entrySet()) {
            if (entry.getValue().getChannel() == channel) {
                Integer opaque = entry.getKey();
                if (opaque != null) {
                    requestFail(opaque);
                }
            }
        }
    }

    public void invokeOnewayImpl(final Channel channel, final RemotingCommand request, final long timeoutMillis)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException {
        request.markOnewayRPC();
        boolean acquired = this.semaphoreOneway.tryAcquire(timeoutMillis, TimeUnit.MILLISECONDS);
        if (acquired) {
            final SemaphoreReleaseOnlyOnce once = new SemaphoreReleaseOnlyOnce(this.semaphoreOneway);
            try {
                channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
                    once.release();
                    if (!f.isSuccess()) {
                        log.warn("send a request command to channel <" + channel.remoteAddress() + "> failed.");
                    }
                });
            } catch (Exception e) {
                once.release();
                log.warn("write send a request command to channel <" + channel.remoteAddress() + "> failed.");
                throw new RemotingSendRequestException(RemotingHelper.parseChannelRemoteAddr(channel), e);
            }
        } else {
            if (timeoutMillis <= 0) {
                throw new RemotingTooMuchRequestException("invokeOnewayImpl invoke too fast");
            } else {
                String info = String.format(
                        "invokeOnewayImpl tryAcquire semaphore timeout, %dms, waiting thread nums: %d semaphoreOnewayValue: %d",
                        timeoutMillis,
                        this.semaphoreOneway.getQueueLength(),
                        this.semaphoreOneway.availablePermits()
                );
                log.warn(info);
                throw new RemotingTimeoutException(info);
            }
        }
    }

    /**
     * netty事件执行器
     */
    class NettyEventExecutor extends ServiceThread {
        //NettyEventExecutor 内部使用了一个阻塞队列来存放事件，然后不断从阻塞队列消费事件，根据事件类型触发对应的操作。
        private final LinkedBlockingQueue<NettyEvent> eventQueue = new LinkedBlockingQueue<>();

        // 放入 Netty 事件
        public void putNettyEvent(final NettyEvent event) {
            int currentSize = this.eventQueue.size();
            // 最大大小为 10000
            int maxSize = 10000;
            if (currentSize <= maxSize) {
                this.eventQueue.add(event);
            } else {
                // 超出队列大小直接丢弃...
                log.warn("event queue size [{}] over the limit [{}], so drop this event {}", currentSize, maxSize, event.toString());
            }
        }

        @Override
        public void run() {
            log.info(this.getServiceName() + " service started");
            // 通道监听事件
            final ChannelEventListener listener = NettyRemotingAbstract.this.getChannelEventListener();

            // 循环运行
            while (!this.isStopped()) {
                try {
                    // 消费队列中的事件
                    NettyEvent event = this.eventQueue.poll(3000, TimeUnit.MILLISECONDS);
                    if (event != null && listener != null) {
                        switch (event.getType()) {
                            case IDLE:
                                listener.onChannelIdle(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CLOSE:
                                listener.onChannelClose(event.getRemoteAddr(), event.getChannel());
                                break;
                            case CONNECT:
                                listener.onChannelConnect(event.getRemoteAddr(), event.getChannel());
                                break;
                            case EXCEPTION:
                                listener.onChannelException(event.getRemoteAddr(), event.getChannel());
                                break;
                            default:
                                break;

                        }
                    }
                } catch (Exception e) {
                    log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return NettyEventExecutor.class.getSimpleName();
        }
    }
}
