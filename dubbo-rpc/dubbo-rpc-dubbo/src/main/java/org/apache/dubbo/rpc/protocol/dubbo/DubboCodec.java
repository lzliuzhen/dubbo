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
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.io.Bytes;
import org.apache.dubbo.common.io.UnsafeByteArrayInputStream;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.serialize.ObjectInput;
import org.apache.dubbo.common.serialize.ObjectOutput;
import org.apache.dubbo.common.serialize.Serialization;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.codec.ExchangeCodec;
import org.apache.dubbo.remoting.transport.CodecSupport;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.FrameworkModel;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_VERSION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.DECODE_IN_IO_THREAD_KEY;
import static org.apache.dubbo.rpc.protocol.dubbo.Constants.DEFAULT_DECODE_IN_IO_THREAD;

/**
 * Dubbo codec.
 */
public class DubboCodec extends ExchangeCodec {

    public static final String NAME = "dubbo";
    public static final String DUBBO_VERSION = Version.getProtocolVersion();
    public static final byte RESPONSE_WITH_EXCEPTION = 0;
    public static final byte RESPONSE_VALUE = 1;
    public static final byte RESPONSE_NULL_VALUE = 2;
    public static final byte RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS = 3;
    public static final byte RESPONSE_VALUE_WITH_ATTACHMENTS = 4;
    public static final byte RESPONSE_NULL_VALUE_WITH_ATTACHMENTS = 5;
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    public static final Class<?>[] EMPTY_CLASS_ARRAY = new Class<?>[0];
    private static final Logger log = LoggerFactory.getLogger(DubboCodec.class);
    private CallbackServiceCodec callbackServiceCodec;
    private FrameworkModel frameworkModel;

    public DubboCodec(FrameworkModel frameworkModel) {
        this.frameworkModel = frameworkModel;
        callbackServiceCodec = new CallbackServiceCodec(frameworkModel);
    }

    @Override
    protected Object decodeBody(Channel channel, InputStream is, byte[] header) throws IOException {
        // 对于header[2]就是一个flag，request还是什么
        byte flag = header[2], proto = (byte) (flag & SERIALIZATION_MASK);
        // get request id.
        // 从header里提取出来对应的request id
        long id = Bytes.bytes2long(header, 4);

        // 如果说flag和FLAG_REQUEST进行位运算，0，说明他不是request，是response
        if ((flag & FLAG_REQUEST) == 0) {
            // decode response.
            Response res = new Response(id);
            if ((flag & FLAG_EVENT) != 0) {
                res.setEvent(true);
            }
            // get status.
            byte status = header[3];
            res.setStatus(status);
            try {
                if (status == Response.OK) {
                    Object data;
                    if (res.isEvent()) {
                        byte[] eventPayload = CodecSupport.getPayload(is);
                        if (CodecSupport.isHeartBeat(eventPayload, proto)) {
                            // heart beat response data is always null;
                            data = null;
                        } else {
                            ObjectInput in = CodecSupport.deserialize(channel.getUrl(), new ByteArrayInputStream(eventPayload), proto);
                            data = decodeEventData(channel, in, eventPayload);
                        }
                    } else {
                        DecodeableRpcResult result;
                        if (channel.getUrl().getParameter(DECODE_IN_IO_THREAD_KEY, DEFAULT_DECODE_IN_IO_THREAD)) {
                            result = new DecodeableRpcResult(channel, res, is,
                                    (Invocation) getRequestData(id), proto);
                            result.decode();
                        } else {
                            result = new DecodeableRpcResult(channel, res,
                                    new UnsafeByteArrayInputStream(readMessageData(is)),
                                    (Invocation) getRequestData(id), proto);
                        }
                        data = result;
                    }
                    res.setResult(data);
                } else {
                    ObjectInput in = CodecSupport.deserialize(channel.getUrl(), is, proto);
                    res.setErrorMessage(in.readUTF());
                }
            } catch (Throwable t) {
                if (log.isWarnEnabled()) {
                    log.warn("Decode response failed: " + t.getMessage(), t);
                }
                res.setStatus(Response.CLIENT_ERROR);
                res.setErrorMessage(StringUtils.toString(t));
            }
            return res;
        } else {
            // decode request.
            // 如果说flag和FLAG_REQUEST进行与运算，不为0，request
            Request req = new Request(id);
            req.setVersion(Version.getProtocolVersion()); // version这个东西是代码里面写死的，通用的
            req.setTwoWay((flag & FLAG_TWOWAY) != 0); // 把你的flag和FLAG_TWOWAY进行与运算，如果不为0，就说明是two way
            if ((flag & FLAG_EVENT) != 0) { // 一个字节，用一个字节的二进制的数据，通过各种位运算，就可以让一个字节表达出来各种各样的含义
                req.setEvent(true);
            }
            try {
                Object data;
                if (req.isEvent()) {
                    byte[] eventPayload = CodecSupport.getPayload(is);
                    if (CodecSupport.isHeartBeat(eventPayload, proto)) {
                        // heart beat response data is always null;
                        data = null;
                    } else {
                        ObjectInput in = CodecSupport.deserialize(channel.getUrl(), new ByteArrayInputStream(eventPayload), proto);
                        data = decodeEventData(channel, in, eventPayload);
                    }
                } else {
                    DecodeableRpcInvocation inv;
                    if (channel.getUrl().getParameter(DECODE_IN_IO_THREAD_KEY, DEFAULT_DECODE_IN_IO_THREAD)) {
                        inv = new DecodeableRpcInvocation(frameworkModel, channel, req, is, proto);
                        inv.decode(); // 现在在执行这些逻辑的，是我们的netty的Io线程，是否再io线程里直接进行decode
                    } else {
                        inv = new DecodeableRpcInvocation(frameworkModel, channel, req,
                                new UnsafeByteArrayInputStream(readMessageData(is)), proto);
                    }
                    data = inv;
                }
                req.setData(data);
            } catch (Throwable t) {
                if (log.isWarnEnabled()) {
                    log.warn("Decode request failed: " + t.getMessage(), t);
                }
                // bad request
                req.setBroken(true);
                req.setData(t);
            }

            // 我们可以看到，编码和序列化的时候，这个过程跟我们解码和反序列化是完全对上的
            return req;
        }
    }

    private byte[] readMessageData(InputStream is) throws IOException {
        if (is.available() > 0) {
            byte[] result = new byte[is.available()];
            is.read(result);
            return result;
        }
        return new byte[]{};
    }

    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeRequestData(channel, out, data, DUBBO_VERSION);
    }

    @Override
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data) throws IOException {
        encodeResponseData(channel, out, data, DUBBO_VERSION);
    }

    @Override
    protected void encodeRequestData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        RpcInvocation inv = (RpcInvocation) data;

        out.writeUTF(version); // 先写version
        // https://github.com/apache/dubbo/issues/6138
        String serviceName = inv.getAttachment(INTERFACE_KEY);
        if (serviceName == null) {
            serviceName = inv.getAttachment(PATH_KEY);
        }
        out.writeUTF(serviceName); // 再写服务名称
        out.writeUTF(inv.getAttachment(VERSION_KEY)); // 再version key拿出来的东西

        out.writeUTF(inv.getMethodName()); // 再去写方法名称
        out.writeUTF(inv.getParameterTypesDesc());// 再去写方法参数类型的描述
        Object[] args = inv.getArguments(); // 再去写一些具体的参数值
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                out.writeObject(callbackServiceCodec.encodeInvocationArgument(channel, inv, i));
            }
        }
        out.writeAttachments(inv.getObjectAttachments());
    }

    @Override
    protected void encodeResponseData(Channel channel, ObjectOutput out, Object data, String version) throws IOException {
        Result result = (Result) data;
        // currently, the version value in Response records the version of Request
        boolean attach = Version.isSupportResponseAttachment(version);
        Throwable th = result.getException();
        if (th == null) {
            Object ret = result.getValue();
            if (ret == null) {
                out.writeByte(attach ? RESPONSE_NULL_VALUE_WITH_ATTACHMENTS : RESPONSE_NULL_VALUE);
            } else {
                out.writeByte(attach ? RESPONSE_VALUE_WITH_ATTACHMENTS : RESPONSE_VALUE);
                out.writeObject(ret);
            }
        } else {
            out.writeByte(attach ? RESPONSE_WITH_EXCEPTION_WITH_ATTACHMENTS : RESPONSE_WITH_EXCEPTION);
            out.writeThrowable(th);
        }

        if (attach) {
            // returns current version of Response to consumer side.
            result.getObjectAttachments().put(DUBBO_VERSION_KEY, Version.getProtocolVersion());
            out.writeAttachments(result.getObjectAttachments());
        }
    }

    @Override
    protected Serialization getSerialization(Channel channel, Request req) {
        if (!(req.getData() instanceof Invocation)) {
            return super.getSerialization(channel, req);
        }
        return DubboCodecSupport.getRequestSerialization(channel.getUrl(), (Invocation) req.getData());
    }

    @Override
    protected Serialization getSerialization(Channel channel, Response res) {
        if (!(res.getResult() instanceof AppResponse)) {
            return super.getSerialization(channel, res);
        }
        return DubboCodecSupport.getResponseSerialization(channel.getUrl(), (AppResponse) res.getResult());
    }

}
