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
package org.apache.rocketmq.remoting.protocol;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.base.Stopwatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BoundaryType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.org.slf4j.Logger;
import org.apache.rocketmq.logging.org.slf4j.LoggerFactory;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

/**
 * 通信协议封装
 * RemotingCommand 是 RocketMQ 请求、响应的对象载体，可以理解成是 RocketMQ 定义的网络通信协议。在发送请求时，会根据当前请求和数据构建一个 RemotingCommand 对象，然后经过序列化、编码成字节码，
 * 再经过 Netty 的 Channel 发送出去，另一端则从 Channel 读取字节码，经过解码、反序列化成 RemotingCommand。
 * <p>
 * RocketMQ 将消息分为请求头和请求体两部分，请求头就是 RemotingCommand 中可序列化的字段属性，请求体就是 body 字节数组
 */
public class RemotingCommand {
    public static final String SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";
    public static final String SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";
    public static final String REMOTING_VERSION_KEY = "rocketmq.remoting.version";
    static final Logger log = LoggerFactory.getLogger(LoggerName.ROCKETMQ_REMOTING_NAME);
    private static final int RPC_TYPE = 0; // 0, REQUEST_COMMAND
    private static final int RPC_ONEWAY = 1; // 0, RPC
    private static final Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP =
            new HashMap<>();
    private static final Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<>();
    // 1, Oneway
    // 1, RESPONSE_COMMAND
    private static final Map<Field, Boolean> NULLABLE_FIELD_CACHE = new HashMap<>();
    private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();
    private static final String BOUNDARY_TYPE_CANONICAL_NAME = BoundaryType.class.getCanonicalName();
    private static volatile int configVersion = -1;

    private static AtomicInteger requestId = new AtomicInteger(0);

    // 序列化类型 JSON
    private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;

    static {
        // 从配置文件或环境变量中读取序列化类型配置
        final String protocol = System.getProperty(SERIALIZE_TYPE_PROPERTY, System.getenv(SERIALIZE_TYPE_ENV));
        if (!StringUtils.isBlank(protocol)) {
            try {
                serializeTypeConfigInThisServer = SerializeType.valueOf(protocol);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("parser specified protocol error. protocol=" + protocol, e);
            }
        }
    }

    // 请求编码，RocketMQ 中每个API都有一个编码，服务端接收到这个请求之后根据编码来分发。这个东西就类似于 HTTP 请求的 URL 路径
    private int code;
    // 客户端的编程语言，默认是 JAVA，其它的比如 CPP、DOTNET、PYTHON、GO 等等
    private LanguageCode language = LanguageCode.JAVA;
    //发送端 RocketMQ 本身的版本号，这个版本号主要用于不同 Client、Broker、NameServer 版本之间的一些兼容处理。
    private int version = 0;
    // 每个请求都有一个ID，它是用一个全局的 AtomicInteger 自增实现的
    private int opaque = requestId.getAndIncrement();
    // 请求类型，有 Request、Response、Oneway 三种类型
    private int flag = 0;
    // 备注
    private String remark;
    // 在序列化时，会反射解析 customHeader 对象中的字段和数据，然后放入 extFields 表中
    private HashMap<String, String> extFields;
    // 自定义请求头，需要自定义一个VO实现 CommandCustomHeader 接口，然后创建这个对象来封装数据。注意它是 transient 标识的，不会被序列化
    private transient CommandCustomHeader customHeader;

    //序列化的类型，有 JSON、ROCKETMQ 两种；通过这个字段，Server 端就可以知道 Client 端用的什么方式来序列化，然后就用相同的方式来反序列化。
    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;

    // 要发送的消息主体，注意它是byte数组，需要先将对象序列化成字节数组后再设置进来。它也是 transient 标识的，不会被序列化
    private transient byte[] body;
    private boolean suspended;
    private Stopwatch processTimer;

    protected RemotingCommand() {
    }

    /**
     * 创建请求对象
     *
     * @param code
     * @param customHeader
     * @return
     */
    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        //请求编码
        cmd.setCode(code);
        //自定义请求头
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }

    /**
     * 创建响应
     *
     * @param code
     * @param customHeader
     * @return
     */
    public static RemotingCommand createResponseCommandWithHeader(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        // 请求编码
        cmd.setCode(code);
        // 设置为响应类型（默认为请求类型）
        cmd.markResponseType();
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }

    protected static void setCmdVersion(RemotingCommand cmd) {
        if (configVersion >= 0) {
            cmd.setVersion(configVersion);
        } else {
            String v = System.getProperty(REMOTING_VERSION_KEY);
            if (v != null) {
                int value = Integer.parseInt(v);
                cmd.setVersion(value);
                configVersion = value;
            }
        }
    }

    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code", classHeader);
    }

    public static RemotingCommand buildErrorResponse(int code, String remark,
                                                     Class<? extends CommandCustomHeader> classHeader) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(classHeader);
        response.setCode(code);
        response.setRemark(remark);
        return response;
    }

    public static RemotingCommand buildErrorResponse(int code, String remark) {
        return buildErrorResponse(code, remark, null);
    }

    public static RemotingCommand createResponseCommand(int code, String remark,
                                                        Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.markResponseType();
        cmd.setCode(code);
        cmd.setRemark(remark);
        setCmdVersion(cmd);

        if (classHeader != null) {
            try {
                CommandCustomHeader objectHeader = classHeader.getDeclaredConstructor().newInstance();
                cmd.customHeader = objectHeader;
            } catch (InstantiationException e) {
                return null;
            } catch (IllegalAccessException e) {
                return null;
            } catch (InvocationTargetException e) {
                return null;
            } catch (NoSuchMethodException e) {
                return null;
            }
        }

        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        return createResponseCommand(code, remark, null);
    }

    public static RemotingCommand decode(final byte[] array) throws RemotingCommandException {
        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        return decode(byteBuffer);
    }

    public static RemotingCommand decode(final ByteBuffer byteBuffer) throws RemotingCommandException {
        return decode(Unpooled.wrappedBuffer(byteBuffer));
    }

    /**
     * 它首先读取数据的总长度，然后读取前四个字节（注意最开始的前四个字节存储的数据总长度，已经去掉），然后从这个整数中去读取请求头的长度和序列化的方式，接着就是对请求头反序列化得到 RemotingCommand。最后，
     * 总长度减去请求头的长度，以及前4个字节，剩下的就是请求体 body 了。
     *
     * @param byteBuffer
     * @return
     * @throws RemotingCommandException
     */
    public static RemotingCommand decode(final ByteBuf byteBuffer) throws RemotingCommandException {
        // 数据总长度
        int length = byteBuffer.readableBytes();
        // 读取第4-8字节（前4个字节标识总长度，已被去掉）
        int oriHeaderLen = byteBuffer.readInt();
        // 请求头的长度
        int headerLength = getHeaderLength(oriHeaderLen);
        if (headerLength > length - 4) {
            throw new RemotingCommandException("decode error, bad header length: " + headerLength);
        }

        RemotingCommand cmd = headerDecode(byteBuffer, headerLength, getProtocolType(oriHeaderLen));

        // 剩下的就是 body 部分
        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.readBytes(bodyData);
        }
        cmd.body = bodyData;

        return cmd;
    }

    public static int getHeaderLength(int length) {
        return length & 0xFFFFFF;
    }

    private static RemotingCommand headerDecode(ByteBuf byteBuffer, int len,
                                                SerializeType type) throws RemotingCommandException {
        // 序列化类型
        switch (type) {
            case JSON:
                byte[] headerData = new byte[len];
                // 读取指定长度的字节到字节数组中
                byteBuffer.readBytes(headerData);
                // JSON 反序列化
                RemotingCommand resultJson = RemotingSerializable.decode(headerData, RemotingCommand.class);
                resultJson.setSerializeTypeCurrentRPC(type);
                return resultJson;
            case ROCKETMQ:
                // ROCKETMQ 反序列化
                RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(byteBuffer, len);
                resultRMQ.setSerializeTypeCurrentRPC(type);
                return resultRMQ;
            default:
                break;
        }

        return null;
    }

    public static SerializeType getProtocolType(int source) {
        return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
    }

    public static int createNewRequestId() {
        return requestId.getAndIncrement();
    }

    public static SerializeType getSerializeTypeConfigInThisServer() {
        return serializeTypeConfigInThisServer;
    }

    public static int markProtocolType(int source, SerializeType type) {
        return (type.getCode() << 24) | (source & 0x00FFFFFF);
    }

    public void markResponseType() {
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    public CommandCustomHeader readCustomHeader() {
        return customHeader;
    }

    public void writeCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }

    /**
     * 进行解码操作
     *
     * @param classHeader
     * @return
     * @throws RemotingCommandException
     */
    public CommandCustomHeader decodeCommandCustomHeader(
            Class<? extends CommandCustomHeader> classHeader) throws RemotingCommandException {
        return decodeCommandCustomHeader(classHeader, true);
    }

    /**
     * 解码操作
     *
     * @param classHeader
     * @param useFastEncode
     * @return
     * @throws RemotingCommandException
     */
    public CommandCustomHeader decodeCommandCustomHeader(Class<? extends CommandCustomHeader> classHeader,
                                                         boolean useFastEncode) throws RemotingCommandException {
        //构建CommandCustomHeader实例
        CommandCustomHeader objectHeader;
        try {
            objectHeader = classHeader.getDeclaredConstructor().newInstance();
        } catch (InstantiationException e) {
            return null;
        } catch (IllegalAccessException e) {
            return null;
        } catch (InvocationTargetException e) {
            return null;
        } catch (NoSuchMethodException e) {
            return null;
        }

        if (this.extFields != null) {
            if (objectHeader instanceof FastCodesHeader && useFastEncode) {
                ((FastCodesHeader) objectHeader).decode(this.extFields);
                objectHeader.checkFields();
                return objectHeader;
            }

            Field[] fields = getClazzFields(classHeader);
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String fieldName = field.getName();
                    if (!fieldName.startsWith("this")) {
                        try {
                            String value = this.extFields.get(fieldName);
                            if (null == value) {
                                if (!isFieldNullable(field)) {
                                    throw new RemotingCommandException("the custom field <" + fieldName + "> is null");
                                }
                                continue;
                            }

                            field.setAccessible(true);
                            String type = getCanonicalName(field.getType());
                            Object valueParsed;

                            if (type.equals(STRING_CANONICAL_NAME)) {
                                valueParsed = value;
                            } else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
                                valueParsed = Integer.parseInt(value);
                            } else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
                                valueParsed = Long.parseLong(value);
                            } else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
                                valueParsed = Boolean.parseBoolean(value);
                            } else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
                                valueParsed = Double.parseDouble(value);
                            } else if (type.equals(BOUNDARY_TYPE_CANONICAL_NAME)) {
                                valueParsed = BoundaryType.getType(value);
                            } else {
                                throw new RemotingCommandException("the custom field <" + fieldName + "> type is not supported");
                            }

                            field.set(objectHeader, valueParsed);

                        } catch (Throwable e) {
                            log.error("Failed field [{}] decoding", fieldName, e);
                        }
                    }
                }
            }

            objectHeader.checkFields();
        }

        return objectHeader;
    }

    //make it able to test
    Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
        Field[] field = CLASS_HASH_MAP.get(classHeader);

        if (field == null) {
            Set<Field> fieldList = new HashSet<>();
            for (Class className = classHeader; className != Object.class; className = className.getSuperclass()) {
                Field[] fields = className.getDeclaredFields();
                fieldList.addAll(Arrays.asList(fields));
            }
            field = fieldList.toArray(new Field[0]);
            synchronized (CLASS_HASH_MAP) {
                CLASS_HASH_MAP.put(classHeader, field);
            }
        }
        return field;
    }

    private boolean isFieldNullable(Field field) {
        if (!NULLABLE_FIELD_CACHE.containsKey(field)) {
            Annotation annotation = field.getAnnotation(CFNotNull.class);
            synchronized (NULLABLE_FIELD_CACHE) {
                NULLABLE_FIELD_CACHE.put(field, annotation == null);
            }
        }
        return NULLABLE_FIELD_CACHE.get(field);
    }

    private String getCanonicalName(Class clazz) {
        String name = CANONICAL_NAME_CACHE.get(clazz);

        if (name == null) {
            name = clazz.getCanonicalName();
            synchronized (CANONICAL_NAME_CACHE) {
                CANONICAL_NAME_CACHE.put(clazz, name);
            }
        }
        return name;
    }

    public ByteBuffer encode() {
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData = this.headerEncode();
        length += headerData.length;

        // 3> body data length
        if (this.body != null) {
            length += body.length;
        }

        ByteBuffer result = ByteBuffer.allocate(4 + length);

        // length
        result.putInt(length);

        // header length
        result.putInt(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        // body data;
        if (this.body != null) {
            result.put(this.body);
        }

        result.flip();

        return result;
    }

    private byte[] headerEncode() {
        this.makeCustomHeaderToNet();
        if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
            return RocketMQSerializable.rocketMQProtocolEncode(this);
        } else {
            return RemotingSerializable.encode(this);
        }
    }

    /**
     * 不管是哪种序列化方式，如果传入了自定义请求头 CommandCustomHeader，就会将这个对象中的字段和值解析出来，放入 extFields 扩展字段表中。可以看到它就是通过反射的方式，获取对象中的所有字段，然后通过反射的方式获取字段值。
     */
    public void makeCustomHeaderToNet() {
        if (this.customHeader != null) {
            // 获取自定义请求头类中的字段
            Field[] fields = getClazzFields(customHeader.getClass());
            if (null == this.extFields) {
                this.extFields = new HashMap<>();
            }

            for (Field field : fields) {
                // 非静态字段
                if (!Modifier.isStatic(field.getModifiers())) {
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object value = null;
                        try {
                            field.setAccessible(true);
                            // 反射读值
                            value = field.get(this.customHeader);
                        } catch (Exception e) {
                            log.error("Failed to access field [{}]", name, e);
                        }
                        // 添加到 extFields
                        if (value != null) {
                            this.extFields.put(name, value.toString());
                        }
                    }
                }
            }
        }
    }

    public void fastEncodeHeader(ByteBuf out) {
        // 消息主体的字节长度
        int bodySize = this.body != null ? this.body.length : 0;
        // 初始位置
        int beginIndex = out.writerIndex();
        // 先占用8个字节
        // skip 8 bytes
        out.writeLong(0);
        int headerSize;
        // ROCKETMQ 序列化
        if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
            if (customHeader != null && !(customHeader instanceof FastCodesHeader)) {
                // customHeader 转到 extFields
                this.makeCustomHeaderToNet();
            }
            // ROCKETMQ 序列化
            headerSize = RocketMQSerializable.rocketMQProtocolEncode(this, out);
        }
        // JSON 序列化
        else {
            // customHeader 转到 extFields
            this.makeCustomHeaderToNet();
            // JSON 序列化
            byte[] header = RemotingSerializable.encode(this);
            headerSize = header.length;
            // 直接写入字节
            out.writeBytes(header);
        }
        // 前4个字节写入消息的长度
        out.setInt(beginIndex, 4 + headerSize + bodySize);
        // 4-8字节写入序列化的类型和请求头长度
        out.setInt(beginIndex + 4, markProtocolType(headerSize, serializeTypeCurrentRPC));
    }

    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    public ByteBuffer encodeHeader(final int bodyLength) {
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData;
        headerData = this.headerEncode();

        length += headerData.length;

        // 3> body data length
        length += bodyLength;

        ByteBuffer result = ByteBuffer.allocate(4 + length - bodyLength);

        // length
        result.putInt(length);

        // header length
        result.putInt(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        ((Buffer) result).flip();

        return result;
    }

    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits) == bits;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }

        return RemotingCommandType.REQUEST_COMMAND;
    }

    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }

    public LanguageCode getLanguage() {
        return language;
    }

    public void setLanguage(LanguageCode language) {
        this.language = language;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public int getOpaque() {
        return opaque;
    }

    public void setOpaque(int opaque) {
        this.opaque = opaque;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public String getRemark() {
        return remark;
    }

    public void setRemark(String remark) {
        this.remark = remark;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    @JSONField(serialize = false)
    public boolean isSuspended() {
        return suspended;
    }

    @JSONField(serialize = false)
    public void setSuspended(boolean suspended) {
        this.suspended = suspended;
    }

    public HashMap<String, String> getExtFields() {
        return extFields;
    }

    public void setExtFields(HashMap<String, String> extFields) {
        this.extFields = extFields;
    }

    public void addExtField(String key, String value) {
        if (null == extFields) {
            extFields = new HashMap<>(256);
        }
        extFields.put(key, value);
    }

    public void addExtFieldIfNotExist(String key, String value) {
        extFields.putIfAbsent(key, value);
    }

    @Override
    public String toString() {
        return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque=" + opaque + ", flag(B)="
                + Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields=" + extFields + ", serializeTypeCurrentRPC="
                + serializeTypeCurrentRPC + "]";
    }

    public SerializeType getSerializeTypeCurrentRPC() {
        return serializeTypeCurrentRPC;
    }

    public void setSerializeTypeCurrentRPC(SerializeType serializeTypeCurrentRPC) {
        this.serializeTypeCurrentRPC = serializeTypeCurrentRPC;
    }

    public Stopwatch getProcessTimer() {
        return processTimer;
    }

    public void setProcessTimer(Stopwatch processTimer) {
        this.processTimer = processTimer;
    }
}
