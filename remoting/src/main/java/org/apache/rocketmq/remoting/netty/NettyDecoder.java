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

import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 解码器
 * <p>
 * 这是输入时的一个处理器，NettyDecoder 负责在接收请求时对 ByteBuf 解码，将其转成 RemotingCommand。
 * <p>
 * NettyDecoder 继承自 LengthFieldBasedFrameDecoder，也就是说它是基于固定长度的一个解码器。可以看到默认长度为 16777216，也就是 16M，就是说单次请求最大不能超过 16M 的数据，
 * 否则就会报错，当然可以通过参数 com.rocketmq.remoting.frameMaxLength 修改这个限制。
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    private static final int FRAME_MAX_LENGTH =
            Integer.parseInt(System.getProperty("com.rocketmq.remoting.frameMaxLength", "16777216"));

    public NettyDecoder() {
        //从构造方法可以看出，消息的前4个字节用来表示消息的总长度，所以在编码的时候，前 4 个字节一定会写入消息的总长度，然后它的第五个参数（initialBytesToStrip）表示要去掉前4个字节，所以在 decode 中拿到的 ByteBuf 是不包含这个总长度的。
        // 从0开始，用4个字节来表示长度，解码时删除前4个字节
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    @Override
    public Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        Stopwatch timer = Stopwatch.createStarted();
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (null == frame) {
                return null;
            }
            // ByteBuf 解码为 RemotingCommand
            RemotingCommand cmd = RemotingCommand.decode(frame);
            cmd.setProcessTimer(timer);
            return cmd;
        } catch (Exception e) {
            log.error("decode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            RemotingHelper.closeChannel(ctx.channel());
        } finally {
            if (null != frame) {
                // 释放资源
                frame.release();
            }
        }

        return null;
    }
}
