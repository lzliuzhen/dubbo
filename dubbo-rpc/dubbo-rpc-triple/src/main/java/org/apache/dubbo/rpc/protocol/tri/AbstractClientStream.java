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

package org.apache.dubbo.rpc.protocol.tri;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.stream.StreamObserver;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.api.Connection;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture2;
import org.apache.dubbo.rpc.CancellationContext;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.model.ConsumerModel;
import org.apache.dubbo.rpc.model.MethodDescriptor;
import org.apache.dubbo.rpc.model.ServiceModel;
import org.apache.dubbo.triple.TripleWrapper;

import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2StreamChannel;
import io.netty.util.AsciiString;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import static org.apache.dubbo.rpc.Constants.COMPRESSOR_KEY;
import static org.apache.dubbo.rpc.protocol.tri.Compressor.DEFAULT_COMPRESSOR;


public abstract class AbstractClientStream extends AbstractStream implements Stream {

    private final AsciiString scheme;
    private ConsumerModel consumerModel;
    private Connection connection;
    private RpcInvocation rpcInvocation;
    private long requestId;

    protected AbstractClientStream(URL url) {
        super(url);
        this.scheme = getSchemeFromUrl(url);
        // for client cancel,send rst frame to server
        this.getCancellationContext().addListener(context -> {
            Throwable throwable = this.getCancellationContext().getCancellationCause();
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Triple request to "
                    + getConsumerModel().getServiceName() + "#" + getMethodName() +
                    " was canceled by local exception ", throwable);
            }
            this.asTransportObserver().onReset(getHttp2Error(throwable));
        });
    }


    public static UnaryClientStream unary(URL url) {
        return new UnaryClientStream(url);
    }

    public static ClientStream stream(URL url) {
        return new ClientStream(url);
    }

    public static AbstractClientStream newClientStream(Request req, Connection connection) {
        final RpcInvocation inv = (RpcInvocation) req.getData();
        final URL url = inv.getInvoker().getUrl();
        ConsumerModel consumerModel = inv.getServiceModel() != null ? (ConsumerModel) inv.getServiceModel() : (ConsumerModel) url.getServiceModel();
        MethodDescriptor methodDescriptor = getTriMethodDescriptor(consumerModel, inv);
        ClassLoadUtil.switchContextLoader(consumerModel.getClassLoader());
        AbstractClientStream stream = methodDescriptor.isUnary() ? unary(url) : stream(url);
        Compressor compressor = getCompressor(url, consumerModel);
        stream.request(req)
            .service(consumerModel)
            .connection(connection)
            .serialize((String) inv.getObjectAttachment(Constants.SERIALIZATION_KEY))
            .method(methodDescriptor)
            .setCompressor(compressor);
        return stream;
    }

    protected void startCall(Http2StreamChannel channel, ChannelPromise promise) {
        execute(() -> {
            channel.pipeline()
                .addLast(new TripleHttp2ClientResponseHandler())
                .addLast(new GrpcDataDecoder(Integer.MAX_VALUE, true))
                .addLast(new TripleClientInboundHandler());
            channel.attr(TripleConstant.CLIENT_STREAM_KEY).set(this);
            final ClientTransportObserver clientTransportObserver = new ClientTransportObserver(channel, promise);
            subscribe(clientTransportObserver);
            try {
                doOnStartCall();
            } catch (Throwable throwable) {
                cancel(throwable);
                DefaultFuture2.getFuture(getRequestId()).cancel();
            }
        });
    }

    protected abstract void doOnStartCall();

    @Override
    protected StreamObserver<Object> createStreamObserver() {
        return new ClientStreamObserverImpl(getCancellationContext());
    }

    protected class ClientStreamObserverImpl extends CancelableStreamObserver<Object> implements ClientStreamObserver<Object> {

        public ClientStreamObserverImpl(CancellationContext cancellationContext) {
            super(cancellationContext);
        }

        @Override
        public void onNext(Object data) {
            if (getState().allowSendMeta()) {
                final Metadata metadata = createRequestMeta(getRpcInvocation());
                getTransportSubscriber().onMetadata(metadata, false);
            }
            if (getState().allowSendData()) {
                final byte[] bytes = encodeRequest(data);
                getTransportSubscriber().onData(bytes, false);
            }
        }

        /**
         * Handle all exceptions in the request process, other procedures directly throw
         * <p>
         * other procedures is {@link ClientStreamObserver#onNext(Object)} and {@link ClientStreamObserver#onCompleted()}
         */
        @Override
        public void onError(Throwable throwable) {
            if (getState().allowSendEndStream()) {
                GrpcStatus status = GrpcStatus.getStatus(throwable);
                transportError(status, null, getState().allowSendMeta());
            } else {
                if (LOGGER.isErrorEnabled()) {
                    LOGGER.error("Triple request to "
                        + getConsumerModel().getServiceName() + "#" + getMethodName() +
                        " was failed by exception ", throwable);
                }
            }
        }

        @Override
        public void onCompleted() {
            if (getState().allowSendEndStream()) {
                getTransportSubscriber().onComplete();
            }
        }

        @Override
        public void setCompression(String compression) {
            if (!getState().allowSendMeta()) {
                cancel(new IllegalStateException("Metadata already has been sent,can not set compression"));
                return;
            }
            Compressor compressor = Compressor.getCompressor(getUrl().getOrDefaultFrameworkModel(), compression);
            setCompressor(compressor);
        }
    }

    @Override
    protected void cancelByRemoteReset(Http2Error http2Error) {
        DefaultFuture2.getFuture(getRequestId()).cancel();
    }

    @Override
    protected void cancelByLocal(Throwable throwable) {
        getCancellationContext().cancel(throwable);
    }


    @Override
    public void execute(Runnable runnable) {
        try {
            super.execute(runnable);
        } catch (RejectedExecutionException e) {
            LOGGER.error("Consumer's thread pool is full", e);
            getStreamSubscriber().onError(GrpcStatus.fromCode(GrpcStatus.Code.RESOURCE_EXHAUSTED)
                .withDescription("Consumer's thread pool is full").asException());
        } catch (Throwable t) {
            LOGGER.error("Consumer submit request to thread pool error ", t);
            getStreamSubscriber().onError(GrpcStatus.fromCode(GrpcStatus.Code.INTERNAL)
                .withCause(t)
                .withDescription("Consumer's error")
                .asException());
        }
    }

    public AbstractClientStream service(ConsumerModel model) {
        this.consumerModel = model;
        return this;
    }

    public AbstractClientStream request(Request request) {
        this.requestId = request.getId();
        this.rpcInvocation = (RpcInvocation) request.getData();
        return this;
    }

    protected RpcInvocation getRpcInvocation() {
        return this.rpcInvocation;
    }

    public AsciiString getScheme() {
        return scheme;
    }

    public long getRequestId() {
        return requestId;
    }

    private AsciiString getSchemeFromUrl(URL url) {
        try {
            Boolean ssl = url.getParameter(CommonConstants.SSL_ENABLED_KEY, Boolean.class);
            if (ssl == null) {
                return TripleConstant.HTTP_SCHEME;
            }
            return ssl ? TripleConstant.HTTPS_SCHEME : TripleConstant.HTTP_SCHEME;
        } catch (Exception e) {
            return TripleConstant.HTTP_SCHEME;
        }
    }

    private Http2Error getHttp2Error(Throwable throwable) {
        // todo Convert the exception to http2Error
        return Http2Error.CANCEL;
    }

    public ConsumerModel getConsumerModel() {
        return consumerModel;
    }

    public AbstractClientStream connection(Connection connection) {
        this.connection = connection;
        return this;
    }

    public Connection getConnection() {
        return connection;
    }

    protected byte[] encodeRequest(Object value) {
        final byte[] out;
        final Object obj;

        if (getMethodDescriptor().isNeedWrap()) {
            obj = getRequestWrapper(value);
        } else {
            obj = getRequestValue(value);
        }
        out = TripleUtil.pack(obj);
        return super.compress(out);
    }

    private TripleWrapper.TripleRequestWrapper getRequestWrapper(Object value) {
        if (getMethodDescriptor().isStream()) {
            String type = getMethodDescriptor().getParameterClasses()[0].getName();
            return TripleUtil.wrapReq(getUrl(), getSerializeType(), value, type, getMultipleSerialization());
        } else {
            RpcInvocation invocation = (RpcInvocation) value;
            return TripleUtil.wrapReq(getUrl(), invocation, getMultipleSerialization());
        }
    }

    private Object getRequestValue(Object value) {
        if (getMethodDescriptor().isUnary()) {
            RpcInvocation invocation = (RpcInvocation) value;
            return invocation.getArguments()[0];
        }
        return value;
    }

    protected Object deserializeResponse(byte[] data) {
        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        try {
            if (getConsumerModel() != null) {
                ClassLoadUtil.switchContextLoader(getConsumerModel().getClassLoader());
            }
            if (getMethodDescriptor().isNeedWrap()) {
                final TripleWrapper.TripleResponseWrapper wrapper = TripleUtil.unpack(data,
                    TripleWrapper.TripleResponseWrapper.class);
                if (!getSerializeType().equals(TripleUtil.convertHessianFromWrapper(wrapper.getSerializeType()))) {
                    throw new UnsupportedOperationException("Received inconsistent serialization type from server, " +
                        "reject to deserialize! Expected:" + getSerializeType() +
                        " Actual:" + TripleUtil.convertHessianFromWrapper(wrapper.getSerializeType()));
                }
                return TripleUtil.unwrapResp(getUrl(), wrapper, getMultipleSerialization());
            } else {
                return TripleUtil.unpack(data, getMethodDescriptor().getReturnClass());
            }
        } finally {
            ClassLoadUtil.switchContextLoader(tccl);
        }
    }

    protected Metadata createRequestMeta(RpcInvocation inv) {
        Metadata metadata = new DefaultMetadata();
        // put http2 params
        metadata.put(Http2Headers.PseudoHeaderName.SCHEME.value(), this.getScheme())
            .put(Http2Headers.PseudoHeaderName.PATH.value(), getMethodPath(inv))
            .put(Http2Headers.PseudoHeaderName.AUTHORITY.value(), getUrl().getAddress())
            .put(Http2Headers.PseudoHeaderName.METHOD.value(), HttpMethod.POST.asciiName());

        metadata.put(TripleHeaderEnum.CONTENT_TYPE_KEY.getHeader(), TripleConstant.CONTENT_PROTO)
            .put(TripleHeaderEnum.TIMEOUT.getHeader(), inv.get(CommonConstants.TIMEOUT_KEY) + "m")
            .put(HttpHeaderNames.TE, HttpHeaderValues.TRAILERS)
        ;

        metadata.putIfNotNull(TripleHeaderEnum.SERVICE_VERSION.getHeader(), getUrl().getVersion())
            .putIfNotNull(TripleHeaderEnum.CONSUMER_APP_NAME_KEY.getHeader(),
                (String) inv.getObjectAttachments().remove(CommonConstants.APPLICATION_KEY))
            .putIfNotNull(TripleHeaderEnum.CONSUMER_APP_NAME_KEY.getHeader(),
                (String) inv.getObjectAttachments().remove(CommonConstants.REMOTE_APPLICATION_KEY))
            .putIfNotNull(TripleHeaderEnum.SERVICE_GROUP.getHeader(), getUrl().getGroup())
            .putIfNotNull(TripleHeaderEnum.GRPC_ENCODING.getHeader(), getCompressor().getMessageEncoding())
            .putIfNotNull(TripleHeaderEnum.GRPC_ACCEPT_ENCODING.getHeader(), Compressor.getAcceptEncoding(getUrl().getOrDefaultFrameworkModel()));
        final Map<String, Object> attachments = inv.getObjectAttachments();
        if (attachments != null) {
            convertAttachment(metadata, attachments);
        }
        return metadata;
    }

    private String getMethodPath(RpcInvocation inv) {
        return "/" + inv.getObjectAttachment(CommonConstants.PATH_KEY) + "/" + inv.getMethodName();
    }

    private static Compressor getCompressor(URL url, ServiceModel model) {
        String compressorStr = url.getParameter(COMPRESSOR_KEY);
        if (compressorStr == null) {
            // Compressor can not be set by dynamic config
            compressorStr = ConfigurationUtils
                .getCachedDynamicProperty(model.getModuleModel(), COMPRESSOR_KEY, DEFAULT_COMPRESSOR);
        }
        return Compressor.getCompressor(url.getOrDefaultFrameworkModel(), compressorStr);
    }

    /**
     * Get the tri protocol special MethodDescriptor
     */
    private static MethodDescriptor getTriMethodDescriptor(ConsumerModel consumerModel, RpcInvocation inv) {
        List<MethodDescriptor> methodDescriptors = consumerModel.getServiceModel().getMethods(inv.getMethodName());
        if (CollectionUtils.isEmpty(methodDescriptors)) {
            throw new IllegalStateException("methodDescriptors must not be null method=" + inv.getMethodName());
        }
        for (MethodDescriptor methodDescriptor : methodDescriptors) {
            if (Arrays.equals(inv.getParameterTypes(), methodDescriptor.getRealParameterClasses())) {
                return methodDescriptor;
            }
        }
        throw new IllegalStateException("methodDescriptors must not be null method=" + inv.getMethodName());
    }
}
