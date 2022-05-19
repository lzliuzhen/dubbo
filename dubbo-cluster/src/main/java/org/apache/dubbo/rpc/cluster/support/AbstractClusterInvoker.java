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

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.ClusterInvoker;
import org.apache.dubbo.rpc.cluster.Directory;
import org.apache.dubbo.rpc.cluster.LoadBalance;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_LOADBALANCE;
import static org.apache.dubbo.common.constants.CommonConstants.LOADBALANCE_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.CLUSTER_AVAILABLE_CHECK_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.CLUSTER_STICKY_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_CLUSTER_AVAILABLE_CHECK;
import static org.apache.dubbo.rpc.cluster.Constants.DEFAULT_CLUSTER_STICKY;

/**
 * AbstractClusterInvoker
 *
 * invoker这块的设计，我觉得是dubbo里非常漂亮的一个实现
 * 如果你要是对一个目标服务实例进行rpc调用，invoke，负责调用的这个组件，就可以叫做invoker
 * 设计模式，抽出来一个抽象的父类，把一些基础的方法和骨架，都放在了父类里面
 * 定义了一个抽象方法，各个子类具体实现自己的逻辑就可以了，复用的代码逻辑都是走父类就好了
 * 模板方法设计模式
 *
 */
public abstract class AbstractClusterInvoker<T> implements ClusterInvoker<T> {

    private static final Logger logger = LoggerFactory.getLogger(AbstractClusterInvoker.class);

    protected Directory<T> directory;

    protected boolean availablecheck;

    private AtomicBoolean destroyed = new AtomicBoolean(false);

    private volatile Invoker<T> stickyInvoker = null;

    public AbstractClusterInvoker() {
    }

    public AbstractClusterInvoker(Directory<T> directory) {
        this(directory, directory.getUrl());
    }

    public AbstractClusterInvoker(Directory<T> directory, URL url) {
        if (directory == null) {
            throw new IllegalArgumentException("service directory == null");
        }

        this.directory = directory;
        //sticky: invoker.isAvailable() should always be checked before using when availablecheck is true.
        this.availablecheck = url.getParameter(CLUSTER_AVAILABLE_CHECK_KEY, DEFAULT_CLUSTER_AVAILABLE_CHECK);
    }

    @Override
    public Class<T> getInterface() {
        return getDirectory().getInterface();
    }

    @Override
    public URL getUrl() {
        return getDirectory().getConsumerUrl();
    }

    @Override
    public URL getRegistryUrl() {
        return getDirectory().getUrl();
    }

    @Override
    public boolean isAvailable() {
        Invoker<T> invoker = stickyInvoker;
        if (invoker != null) {
            return invoker.isAvailable();
        }
        return getDirectory().isAvailable();
    }

    @Override
    public Directory<T> getDirectory() {
        return directory;
    }

    @Override
    public void destroy() {
        if (destroyed.compareAndSet(false, true)) {
            getDirectory().destroy();
        }
    }

    @Override
    public boolean isDestroyed() {
        return destroyed.get();
    }

    /**
     * Select a invoker using loadbalance policy.</br>
     * a) Firstly, select an invoker using loadbalance. If this invoker is in previously selected list, or,
     * if this invoker is unavailable, then continue step b (reselect), otherwise return the first selected invoker</br>
     * <p>
     * b) Reselection, the validation rule for reselection: selected > available. This rule guarantees that
     * the selected invoker has the minimum chance to be one in the previously selected list, and also
     * guarantees this invoker is available.
     *
     * @param loadbalance load balance policy
     * @param invocation  invocation
     * @param invokers    invoker candidates
     * @param selected    exclude selected invokers or not
     * @return the invoker which will final to do invoke.
     * @throws RpcException exception
     */
    protected Invoker<T> select(LoadBalance loadbalance, Invocation invocation,
                                List<Invoker<T>> invokers, List<Invoker<T>> selected) throws RpcException {

        // invokers不能为空，为空的话就直接返回了
        if (CollectionUtils.isEmpty(invokers)) {
            return null;
        }

        // 调用的方法名称的处理，rpc调用如果是null的话，method方法名就是一个空字符串
        // 否则的话，就是rpc调用里的方法名称
        String methodName = invocation == null ? StringUtils.EMPTY_STRING : invocation.getMethodName();

        // 可能你在看源码的时候，你看不懂，仔细的代码逻辑可以看懂吧
        // 一般来说，根据代码逻辑进行一个大致的推测就知道，这个所谓的sticky invoker应该是在某些特殊情况下
        // 会生效，会基于这个sticky invoker发起rpc调用
        // 但是通常我们分析源码，并没有必要说对特殊的代码过多的关注

        // invokers（服务实例集群地址），先获取第一个，拿到他的url，再去获取到对应的sticky
        // 不知道这个sticky是什么含义，默认的话就是一个false
        boolean sticky = invokers.get(0).getUrl()
                .getMethodParameter(methodName, CLUSTER_STICKY_KEY, DEFAULT_CLUSTER_STICKY);

        //ignore overloaded method
        // 对sticky invoker做了一个处理
        if (stickyInvoker != null && !invokers.contains(stickyInvoker)) {
            stickyInvoker = null;
        }

        //ignore concurrency problem
        if (sticky && stickyInvoker != null && (selected == null || !selected.contains(stickyInvoker))) {
            if (availablecheck && stickyInvoker.isAvailable()) {
                // 此时就直接返回stick invoker
                return stickyInvoker;
            }
        }

        // 就会正式的基于loadbalance去进行负载均衡，选择一个invoker出来
        Invoker<T> invoker = doSelect(loadbalance, invocation, invokers, selected);

        // 如果说选择出一个invoker之后，sticky是true，此时还要把这个sticky invoker设置为选择出来的invoker
        if (sticky) {
            stickyInvoker = invoker;
        }

        return invoker;
    }

    private Invoker<T> doSelect(LoadBalance loadbalance, Invocation invocation,
                                List<Invoker<T>> invokers, List<Invoker<T>> selected) throws RpcException {
        // invokers必须不为空
        if (CollectionUtils.isEmpty(invokers)) {
            return null;
        }
        // 如果invokers的数量就1个，目标provider服务实例就一个，invokers的数量肯定也是就1个，直接返回invokers里的第一个就可以了
        if (invokers.size() == 1) {
            return invokers.get(0);
        }
        // 基于负载均衡的策略和算法，选择出一个invoker
        Invoker<T> invoker = loadbalance.select(invokers, getUrl(), invocation);

        //If the `invoker` is in the  `selected` or invoker is unavailable && availablecheck is true, reselect.
        // selected到底是个什么集合，名字来推断呢？
        // 选择过的invokers列表，之前可能在调用的时候，已经选择过了这个invoker
        // 你好不容易选择出来的invoker是不可用的，也是要重新选择的
        if ((selected != null && selected.contains(invoker))
                || (!invoker.isAvailable() && getUrl() != null && availablecheck)) {
            try {
                // 必然要涉及到进行一次重新的select和选择
                Invoker<T> rInvoker = reselect(loadbalance, invocation, invokers, selected, availablecheck);
                if (rInvoker != null) {
                    invoker = rInvoker;
                } else {
                    //Check the index of current selected invoker, if it's not the last one, choose the one at index+1.
                    // 如果说选来选去都是空，对当前invoker直接选择他的下一个invoker就可以了
                    int index = invokers.indexOf(invoker);
                    try {
                        //Avoid collision
                        invoker = invokers.get((index + 1) % invokers.size());
                    } catch (Exception e) {
                        logger.warn(e.getMessage() + " may because invokers list dynamic change, ignore.", e);
                    }
                }
            } catch (Throwable t) {
                logger.error("cluster reselect fail reason is :" + t.getMessage() + " if can not solve, you can set cluster.availablecheck=false in url", t);
            }
        }

        return invoker;
    }

    /**
     * Reselect, use invokers not in `selected` first, if all invokers are in `selected`,
     * just pick an available one using loadbalance policy.
     *
     * @param loadbalance    load balance policy
     * @param invocation     invocation
     * @param invokers       invoker candidates
     * @param selected       exclude selected invokers or not
     * @param availablecheck check invoker available if true
     * @return the reselect result to do invoke
     * @throws RpcException exception
     */
    private Invoker<T> reselect(LoadBalance loadbalance, Invocation invocation,
                                List<Invoker<T>> invokers, List<Invoker<T>> selected, boolean availablecheck) throws RpcException {

        //Allocating one in advance, this list is certain to be used.
        // 构建一个重选择的invokers列表
        List<Invoker<T>> reselectInvokers = new ArrayList<>(
                invokers.size() > 1 ? (invokers.size() - 1) : invokers.size());

        // First, try picking a invoker not in `selected`.
        // 把不可用的invokers摘出来，而且如果是挑选过的invoker也要摘出来
        // 形成一个新的用于选择的invokers列表
        for (Invoker<T> invoker : invokers) {
            if (availablecheck && !invoker.isAvailable()) {
                continue;
            }
            if (selected == null || !selected.contains(invoker)) {
                reselectInvokers.add(invoker);
            }
        }

        if (!reselectInvokers.isEmpty()) {
            // 在这里针对新的invokers列表，再次用负载均衡的算法，选择一个invoker
            return loadbalance.select(reselectInvokers, getUrl(), invocation);
        }

        // Just pick an available invoker using loadbalance policy
        if (selected != null) {
            // 如果上面的那个reselect列表是空的，选无可选，只能从选择过的invokers里面
            // 矬子里面拔将军
            for (Invoker<T> invoker : selected) {
                // 如果说之前选择过的invoker此时是可用的
                if ((invoker.isAvailable()) // available first
                        && !reselectInvokers.contains(invoker)) {
                    // 再次把选择过的invoker但是此时可用了，放到reselect列表里去
                    reselectInvokers.add(invoker);
                }
            }
        }

        if (!reselectInvokers.isEmpty()) {
            // 要是把选择过的invoker再次筛一遍筛出可用的，放到reselect里面去，有，再次进行负载均衡
            return loadbalance.select(reselectInvokers, getUrl(), invocation);
        }

        return null;
    }

    @Override
    public Result invoke(final Invocation invocation) throws RpcException {
        checkWhetherDestroyed();

        // binding attachments into invocation.
//        Map<String, Object> contextAttachments = RpcContext.getClientAttachment().getObjectAttachments();
//        if (contextAttachments != null && contextAttachments.size() != 0) {
//            ((RpcInvocation) invocation).addObjectAttachmentsIfAbsent(contextAttachments);
//        }

        // 对源码的分析和判断，目标服务实例的集群已经被搞成了invokers
        // invokers到底是在哪里获取到的，就是在之前的DynamicDirectory里获取到的
        List<Invoker<T>> invokers = list(invocation);
        // 他会在这里选择出来对应的负载均衡的策略
        LoadBalance loadbalance = initLoadBalance(invokers, invocation);
        RpcUtils.attachInvocationIdIfAsync(getUrl(), invocation);
        return doInvoke(invocation, invokers, loadbalance);
    }

    protected void checkWhetherDestroyed() {
        // 当前服务实例是否说被销毁了，如果是的话，就直接抛异常
        if (destroyed.get()) {
            throw new RpcException("Rpc cluster invoker for " + getInterface() + " on consumer " + NetUtils.getLocalHost()
                    + " use dubbo version " + Version.getVersion()
                    + " is now destroyed! Can not invoke any more.");
        }
    }

    @Override
    public String toString() {
        return getInterface() + " -> " + getUrl().toString();
    }

    protected void checkInvokers(List<Invoker<T>> invokers, Invocation invocation) {
        // 看一下invokers是否为空，如果是空的话，立刻抛错
        if (CollectionUtils.isEmpty(invokers)) {
            throw new RpcException(RpcException.NO_INVOKER_AVAILABLE_AFTER_FILTER, "Failed to invoke the method "
                    + invocation.getMethodName() + " in the service " + getInterface().getName()
                    + ". No provider available for the service " + getDirectory().getConsumerUrl().getServiceKey()
                    + " from registry " + getDirectory().getUrl().getAddress()
                    + " on the consumer " + NetUtils.getLocalHost()
                    + " using the dubbo version " + Version.getVersion()
                    + ". Please check if the providers have been started and registered.");
        }
    }

    protected Result invokeWithContext(Invoker<T> invoker, Invocation invocation) {
        setContext(invoker);
        Result result;
        try {
            result = invoker.invoke(invocation);
        } finally {
            clearContext(invoker);
        }
        return result;
    }

    /**
     * When using a thread pool to fork a child thread, ThreadLocal cannot be passed.
     * In this scenario, please use the invokeWithContextAsync method.
     * @return
     */
    protected Result invokeWithContextAsync(Invoker<T> invoker, Invocation invocation, URL consumerUrl) {
       setContext(invoker, consumerUrl);
        Result result;
        try {
            result = invoker.invoke(invocation);
        } finally {
            clearContext(invoker);
        }
       return result;
    }

    // 抽象方法，留着给子类来实现的
    protected abstract Result doInvoke(Invocation invocation, List<Invoker<T>> invokers,
                                       LoadBalance loadbalance) throws RpcException;

    protected List<Invoker<T>> list(Invocation invocation) throws RpcException {
        return getDirectory().list(invocation);
    }

    /**
     * Init LoadBalance.
     * <p>
     * if invokers is not empty, init from the first invoke's url and invocation
     * if invokes is empty, init a default LoadBalance(RandomLoadBalance)
     * </p>
     *
     * @param invokers   invokers
     * @param invocation invocation
     * @return LoadBalance instance. if not need init, return null.
     */
    protected LoadBalance initLoadBalance(List<Invoker<T>> invokers, Invocation invocation) {
        ApplicationModel applicationModel = ScopeModelUtil.getApplicationModel(invocation.getModuleModel());
        if (CollectionUtils.isNotEmpty(invokers)) {
            return applicationModel.getExtensionLoader(LoadBalance.class).getExtension(
                    invokers.get(0).getUrl().getMethodParameter(
                            RpcUtils.getMethodName(invocation), LOADBALANCE_KEY, DEFAULT_LOADBALANCE
                    )
            );
        } else {
            return applicationModel.getExtensionLoader(LoadBalance.class).getExtension(DEFAULT_LOADBALANCE);
        }
    }


    private void setContext(Invoker<T> invoker) {
        setContext(invoker, null);
    }

    private void setContext(Invoker<T> invoker, URL consumerUrl) {
        RpcContext context = RpcContext.getServiceContext();
        context.setInvoker(invoker)
            .setConsumerUrl(null != consumerUrl ? consumerUrl : RpcContext.getServiceContext().getConsumerUrl());
    }

    private void clearContext(Invoker<T> invoker) {
        // do nothing
        RpcContext context = RpcContext.getServiceContext();
        context.setInvoker(null);
    }
}
