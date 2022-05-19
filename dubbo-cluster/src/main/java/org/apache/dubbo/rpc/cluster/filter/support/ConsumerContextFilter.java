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
package org.apache.dubbo.rpc.cluster.filter.support;

import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.PenetrateAttachmentSelector;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.TimeoutCountDown;
import org.apache.dubbo.rpc.cluster.filter.ClusterFilter;
import org.apache.dubbo.rpc.model.ApplicationModel;

import java.util.Map;
import java.util.Set;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_APPLICATION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIME_COUNTDOWN_KEY;

/**
 * ConsumerContextFilter set current RpcContext with invoker,invocation, local host, remote host and port
 * for consumer invoker.It does it to make the requires info available to execution thread's RpcContext.
 *
 * @see Filter
 * @see RpcContext
 */
@Activate(group = CONSUMER, order = Integer.MIN_VALUE)
public class ConsumerContextFilter implements ClusterFilter, ClusterFilter.Listener {

    private ApplicationModel applicationModel;

    public ConsumerContextFilter(ApplicationModel applicationModel) {
        this.applicationModel = applicationModel;
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        // RpcInvocation，代表了一次rpc调用
        // RpcStatus，代表了你对指定服务、方法调用的统计状态数据
        // RpcContext，代表了rpc调用过程中的一个上下文，就代表了rpc相关的一些东西
        RpcContext.getServiceContext() // 当前线程自己绑定的一个数据空间，作为他的rpc上下文
                .setInvocation(invocation) // 当前这个rpc调用放到了里面去，同时有很多线程都在发起rpc访问
                // 怎么可能说一个RpcContext里面就一个rpc掉用，每个线程在执行一个rpc调用，此时每个线程rpc调用是不是应该有自己的上下文
                .setLocalAddress(NetUtils.getLocalHost(), 0);

        RpcContext context = RpcContext.getClientAttachment();
        context.setAttachment(REMOTE_APPLICATION_KEY, invoker.getUrl().getApplication());
        if (invocation instanceof RpcInvocation) {
            ((RpcInvocation) invocation).setInvoker(invoker); // 对rpc invocation设置一下invoker
        }

        // 用SPI机制，获取了接口的extension loader
        ExtensionLoader<PenetrateAttachmentSelector> selectorExtensionLoader = applicationModel.getExtensionLoader(PenetrateAttachmentSelector.class);
        // 直接再这里就可以拿到这个接口支持的所有的实现类，都可以拿到放到这个set集合里来
        Set<String> supportedSelectors = selectorExtensionLoader.getSupportedExtensions();
        if (CollectionUtils.isNotEmpty(supportedSelectors)) {
            for (String supportedSelector : supportedSelectors) {
                // 对每个具体拿到的实现类，实现类的短名称，对每个实现类的短名称，就直接去获取那个短名称对应的extension实例
                // selector.select
                Map<String, Object> selected = selectorExtensionLoader.getExtension(supportedSelector).select();
                if (CollectionUtils.isNotEmptyMap(selected)) {
                    // 就给我们的rpc invocation添加了一个object attachments
                    ((RpcInvocation) invocation).addObjectAttachments(selected);
                }
            }
        } else {
            ((RpcInvocation) invocation).addObjectAttachments(RpcContext.getServerAttachment().getObjectAttachments());
        }

        Map<String, Object> contextAttachments = RpcContext.getClientAttachment().getObjectAttachments();
        if (CollectionUtils.isNotEmptyMap(contextAttachments)) {
            /**
             * invocation.addAttachmentsIfAbsent(context){@link RpcInvocation#addAttachmentsIfAbsent(Map)}should not be used here,
             * because the {@link RpcContext#setAttachment(String, String)} is passed in the Filter when the call is triggered
             * by the built-in retry mechanism of the Dubbo. The attachment to update RpcContext will no longer work, which is
             * a mistake in most cases (for example, through Filter to RpcContext output traceId and spanId and other information).
             */
            ((RpcInvocation) invocation).addObjectAttachments(contextAttachments);
        }

        try {
            // pass default timeout set by end user (ReferenceConfig)
            Object countDown = context.getObjectAttachment(TIME_COUNTDOWN_KEY);
            if (countDown != null) {
                TimeoutCountDown timeoutCountDown = (TimeoutCountDown) countDown;
                if (timeoutCountDown.isExpired()) {
                    return AsyncRpcResult.newDefaultAsyncResult(new RpcException(RpcException.TIMEOUT_TERMINATE,
                            "No time left for making the following call: " + invocation.getServiceName() + "."
                                    + invocation.getMethodName() + ", terminate directly."), invocation);
                }
            }

            // 做了一个remove server context，跟上面的service context，不是一个东西
            RpcContext.removeServerContext();
            return invoker.invoke(invocation);
        } finally {
            // 都执行完毕了以后，此时必然会把你的线程对应的rpc context去做一个释放和清空
            // 也是类似于一次rpc调用的数据和资源的清理和释放
            RpcContext.removeServiceContext();
            RpcContext.removeClientAttachment();
        }
    }

    @Override
    public void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation) {
        // pass attachments to result
        RpcContext.getServerContext().setObjectAttachments(appResponse.getObjectAttachments());
    }

    @Override
    public void onError(Throwable t, Invoker<?> invoker, Invocation invocation) {

    }

}
