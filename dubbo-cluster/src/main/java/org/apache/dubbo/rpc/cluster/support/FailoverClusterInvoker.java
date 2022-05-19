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
package org.apache.dubbo.rpc.cluster.support;

import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_RETRIES;
import static org.apache.dubbo.common.constants.CommonConstants.RETRIES_KEY;

/**
 * When invoke fails, log the initial error and retry other invokers (retry n times, which means at most n different invokers will be invoked)
 * Note that retry causes latency.
 * <p>
 * <a href="http://en.wikipedia.org/wiki/Failover">Failover</a>
 *
 */
public class FailoverClusterInvoker<T> extends AbstractClusterInvoker<T> {

    // 不同的集群容错的策略，通过你的具体的配置，是可以改变使用不同的策略
    // 你还可以根据自己的需求，自定义集群容错的策略，配置到里面去，让dubbo采用你自己的集群容错的策略
    // 策略模式，策略设计模式，如果单单是看策略模式自己的demo代码的实现，是很简单的
    // dubbo这种大名鼎鼎的经典框架，他也会深度的去使用策略模式，他会针对一个环节，去设计多种可替换的不同的策略实现
    // 提供一个默认的策略实现就可以了，同时你还可以配置使用不同的其他的策略

    private static final Logger logger = LoggerFactory.getLogger(FailoverClusterInvoker.class);

    public FailoverClusterInvoker(Directory<T> directory) {
        super(directory);
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    // 这些代码应该都非常的熟悉了，找一个invoker，invocation交给多个invokers里的一个去发起rpc调用
    // loadbalance会上线负载均衡
    public Result doInvoke(Invocation invocation, final List<Invoker<T>> invokers, LoadBalance loadbalance) throws RpcException {
        // 这个仅仅是做了一个引用的赋值
        List<Invoker<T>> copyInvokers = invokers;
        // 检查invokers，copyInvokers这样的一个引用，rpc调用
        checkInvokers(copyInvokers, invocation);
        // 从你的rpc调用里面提取一个method方法信息，明确一下，你要调用的是哪个方法
        String methodName = RpcUtils.getMethodName(invocation);
        // 计算调用次数，不看具体的实现，因为failover，调用，调用一次不行，就进行重试
        // 是不是必须要去计算出来你的最多可以调用的次数，默认来说failover策略，一般不配置的话，默认就是3次
        int len = calculateInvokeTimes(methodName);
        // retry loop.
        RpcException le = null; // last exception.
        // 构建了一个跟invokers数量相等的一个list
        List<Invoker<T>> invoked = new ArrayList<Invoker<T>>(copyInvokers.size()); // invoked invokers.
        // 基于你的总共计算出的调用的次数，搞了一个set，因为比如说最多调用3次，那么最多会调用3个provider服务实例
        Set<String> providers = new HashSet<String>(len);

        // 对len次数进行循环，i的取值，肯定是0、1、2，i=0
        for (int i = 0; i < len; i++) {
            //Reselect before retry to avoid a change of candidate `invokers`.
            //NOTE: if `invokers` changed, then `invoked` also lose accuracy.

            // 到i>0的时候，一定是所谓的第一次调用失败，要开始进行重试了
            if (i > 0) {
                checkWhetherDestroyed();
                // 就是此时的话呢，对调用dynamic directory进行一次invokers列表的刷新
                // 就是为了，你第一次调用都失败了，有可能invokers列表都出现变化了，所以此时必须刷新一下invokers列表
                copyInvokers = list(invocation);
                // check again
                // 再次check一下invokers是否为空
                checkInvokers(copyInvokers, invocation);
            }

            // 第一次调用，select方法，负载均衡，又是很多的算法和策略
            Invoker<T> invoker = select(loadbalance, invocation, copyInvokers, invoked);
            // 此时先尝试用负载均衡算法去选，选出的invoker是选过的或者不可用的，就直接reselect，对选过的找一个可用的，对选出的直接挑选下一个
            invoked.add(invoker);
            RpcContext.getServiceContext().setInvokers((List) invoked);

            try {
                // invoke的源码我们就不用去深究了，肯定会基于这个invoker去发起rpc调用就可以了
                // 这次调用必然会拿到一个result
                Result result = invokeWithContext(invoker, invocation);
                if (le != null && logger.isWarnEnabled()) {
                    logger.warn("Although retry the method " + methodName
                            + " in the service " + getInterface().getName()
                            + " was successful by the provider " + invoker.getUrl().getAddress()
                            + ", but there have been failed providers " + providers
                            + " (" + providers.size() + "/" + copyInvokers.size()
                            + ") from the registry " + directory.getUrl().getAddress()
                            + " on the consumer " + NetUtils.getLocalHost()
                            + " using the dubbo version " + Version.getVersion() + ". Last error is: "
                            + le.getMessage(), le);
                }
                return result;

                // 如果说要是本次rpc调用失败了，必然会有异常抛出来
            } catch (RpcException e) {
                if (e.isBiz()) { // biz exception.
                    throw e;
                }
                le = e;
            } catch (Throwable e) {
                le = new RpcException(e.getMessage(), e);
            } finally {
                // 会把rpc异常做一个设置，本次调用失败的这个invoker的地址
                providers.add(invoker.getUrl().getAddress());
            }
        }

        // 就会抛这个异常，就说明本次rpc彻底失败了
        throw new RpcException(le.getCode(), "Failed to invoke the method "
                + methodName + " in the service " + getInterface().getName()
                + ". Tried " + len + " times of the providers " + providers
                + " (" + providers.size() + "/" + copyInvokers.size()
                + ") from the registry " + directory.getUrl().getAddress()
                + " on the consumer " + NetUtils.getLocalHost() + " using the dubbo version "
                + Version.getVersion() + ". Last error is: "
                + le.getMessage(), le.getCause() != null ? le.getCause() : le);
    }

    private int calculateInvokeTimes(String methodName) {
        // 默认来说，如果第一次调用失败，那么最多会发起两次重试
        // len默认来说是3次
        int len = getUrl().getMethodParameter(methodName, RETRIES_KEY, DEFAULT_RETRIES) + 1;
        RpcContext rpcContext = RpcContext.getClientAttachment();
        // 如果有特殊配置的retry次数的话，就按照配置的来生效
        Object retry = rpcContext.getObjectAttachment(RETRIES_KEY);
        if (retry instanceof Number) {
            len = ((Number) retry).intValue() + 1;
            rpcContext.removeAttachment(RETRIES_KEY);
        }
        if (len <= 0) {
            len = 1;
        }

        return len;
    }

}
