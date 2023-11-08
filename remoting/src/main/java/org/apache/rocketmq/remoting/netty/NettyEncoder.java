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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 编码器
 * NettyEncoder 就是负责在发送请求时对 RemotingCommand 进行编码，将其转成 ByteBuf。
 */
@ChannelHandler.Sharable
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);

    /**
     * 它的逻辑很简单，调用 RemotingCommand 的 fastEncodeHeader 方法编码，将请求头 RemotingCommand 转成字节然后写入 ByteBuf 中。之后再将请求体 body 再写入 ByteBuf 中，注意它这里是将 RemotingCommand 和 body 分开写入的。
     *
     * @param ctx             the {@link ChannelHandlerContext} which this {@link MessageToByteEncoder} belongs to
     * @param remotingCommand the message to encode
     * @param out             the {@link ByteBuf} into which the encoded message will be written
     * @throws Exception
     */
    @Override
    public void encode(ChannelHandlerContext ctx, RemotingCommand remotingCommand, ByteBuf out)
            throws Exception {
        try {
            // 对 RemotingCommand 编码，将数据写入 ByteBuf 中（不包含body）
            remotingCommand.fastEncodeHeader(out);
            // 若果有 body，则写入 body
            byte[] body = remotingCommand.getBody();
            if (body != null) {
                out.writeBytes(body);
            }
        } catch (Exception e) {
            log.error("encode exception, " + RemotingHelper.parseChannelRemoteAddr(ctx.channel()), e);
            if (remotingCommand != null) {
                log.error(remotingCommand.toString());
            }
            // 关闭通道
            RemotingHelper.closeChannel(ctx.channel());
        }
    }
}
