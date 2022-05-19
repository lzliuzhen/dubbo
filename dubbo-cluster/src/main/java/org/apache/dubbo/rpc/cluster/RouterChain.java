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
package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.cluster.router.state.AddrCache;
import org.apache.dubbo.rpc.cluster.router.state.BitList;
import org.apache.dubbo.rpc.cluster.router.state.RouterCache;
import org.apache.dubbo.rpc.cluster.router.state.StateRouter;
import org.apache.dubbo.rpc.cluster.router.state.StateRouterFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.dubbo.rpc.cluster.Constants.ROUTER_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.STATE_ROUTER_KEY;

/**
 * Router chain
 * 路由链条，有一些路由的规则，通过这些路由规则的过滤，可以帮你把可以用来访问的目标服务实例invokers筛选出来
 * 责任链模式，filter链条，invoker链条，router链条
 *
 */
public class RouterChain<T> {
    private static final Logger logger = LoggerFactory.getLogger(RouterChain.class);

    /**
     * full list of addresses from registry, classified by method name.
     */
    private volatile List<Invoker<T>> invokers = Collections.emptyList();

    /**
     * containing all routers, reconstruct every time 'route://' urls change.
     */
    private volatile List<Router> routers = Collections.emptyList();

    /**
     * Fixed router instances: ConfigConditionRouter, TagRouter, e.g.,
     * the rule for each instance may change but the instance will never delete or recreate.
     */
    private List<Router> builtinRouters = Collections.emptyList();

    private List<StateRouter> builtinStateRouters = Collections.emptyList();
    private List<StateRouter> stateRouters = Collections.emptyList();
    private final ExecutorRepository executorRepository;

    protected URL url;

    private AtomicReference<AddrCache<T>> cache = new AtomicReference<>();

    private final Semaphore loopPermit = new Semaphore(1);
    private final Semaphore loopPermitNotify = new Semaphore(1);

    private final ExecutorService loopPool;

    private AtomicBoolean firstBuildCache = new AtomicBoolean(true);

    public static <T> RouterChain<T> buildChain(URL url) {
        return new RouterChain<>(url);
    }

    private RouterChain(URL url) {
        // router是什么概念，路由的概念，就是说你的当前的consumer的服务实例，他到底可以路由到哪些provider服务实例上去
        // 进行后续的访问，这里就需要一套路由规则，router chain，就可以把你的consumer服务实例约定好
        // 到底有哪些provider服务实例是可以路由过去进行访问的

        // 通过ExecutorRepository，线程池存储组件
        // 从这个线程池存储组件里，会获取出来对应的一个新的线程池，model组件体系，以model作为一个入口，访问所有的框架内部的公共使用的组件
        // SPI、deployer、repository、manager，各种公共组件，通过model作为一个入口都可以获取和访问
        executorRepository = url.getOrDefaultApplicationModel().getExtensionLoader(ExecutorRepository.class)
            .getDefaultExtension();
        loopPool = executorRepository.nextExecutorExecutor();

        // 针对router工厂使用的是SPI的自动激活的机制，拿到的一个router工厂的list
        List<RouterFactory> extensionFactories = url.getOrDefaultApplicationModel().getExtensionLoader(RouterFactory.class)
            .getActivateExtension(url, ROUTER_KEY);
        // 遍历所有的router工厂，每个router工厂都获取到一个router，构建出一个router list
        // java8的语法糖在dubbo里有大量的运用
        List<Router> routers = extensionFactories.stream()
            .map(factory -> factory.getRouter(url))
            .sorted(Router::compareTo)
            .collect(Collectors.toList());

        // router链条，就是一个list，通过有序list的封装，构建了一个有顺序的router链条
        // 后续要进行route路由的时候，就按照顺序，一个一个的路由就可以了
        initWithRouters(routers);

        // 通过SPI机制，StateRouterFactory，有状态的router是这个意思
        List<StateRouterFactory> extensionStateRouterFactories = url.getOrDefaultApplicationModel()
            .getExtensionLoader(StateRouterFactory.class)
            .getActivateExtension(url, STATE_ROUTER_KEY);

        List<StateRouter> stateRouters = extensionStateRouterFactories.stream()
            .map(factory -> factory.getRouter(url, this))
            .sorted(StateRouter::compareTo)
            .collect(Collectors.toList());

        // init state routers
        initWithStateRouters(stateRouters);
    }

    /**
     * the resident routers must being initialized before address notification.
     * FIXME: this method should not be public
     */
    public void initWithRouters(List<Router> builtinRouters) {
        this.builtinRouters = builtinRouters;
        this.routers = new ArrayList<>(builtinRouters);
    }

    private void initWithStateRouters(List<StateRouter> builtinRouters) {
        this.builtinStateRouters = builtinRouters;
        this.stateRouters = new ArrayList<>(builtinRouters);
    }

    /**
     * If we use route:// protocol in version before 2.7.0, each URL will generate a Router instance, so we should
     * keep the routers up to date, that is, each time router URLs changes, we should update the routers list, only
     * keep the builtinRouters which are available all the time and the latest notified routers which are generated
     * from URLs.
     *
     * @param routers routers from 'router://' rules in 2.6.x or before.
     */
    public void addRouters(List<Router> routers) { // 后续可以对一个router链条去做一个维护
        List<Router> newRouters = new ArrayList<>();
        newRouters.addAll(builtinRouters);
        newRouters.addAll(routers);
        CollectionUtils.sort(newRouters);
        this.routers = newRouters;
    }

    public void addStateRouters(List<StateRouter> stateRouters) {
        List<StateRouter> newStateRouters = new ArrayList<>();
        newStateRouters.addAll(builtinStateRouters);
        newStateRouters.addAll(stateRouters);
        CollectionUtils.sort(newStateRouters);
        this.stateRouters = newStateRouters;
    }

    public List<Router> getRouters() {
        return routers;
    }

    public List<StateRouter> getStateRouters() {
        return stateRouters;
    }

    /**
     * @param url
     * @param invocation
     * @return
     */
    public List<Invoker<T>> route(URL url, Invocation invocation) {
        AddrCache<T> cache = this.cache.get();
        List<Invoker<T>> finalInvokers = null;

        if (cache != null) {
            // 构建出来了一个bitlist，把我们的invokers封装到了一个bitlist里面去
            BitList<Invoker<T>> finalBitListInvokers = new BitList<>(invokers, false);
            for (StateRouter stateRouter : stateRouters) {
                if (stateRouter.isEnable()) {
                    // 每一个state router的名称，就对应着一个router cache
                    RouterCache<T> routerCache = cache.getCache().get(stateRouter.getName());
                    finalBitListInvokers = stateRouter.route(finalBitListInvokers, routerCache, url, invocation);
                }
            }

            // 对我们的bitlist invokers，使用state router都执行完路由操作以后，就会把剩余的数据封装为一个普通通的final invokers list
            finalInvokers = new ArrayList<>(finalBitListInvokers.size());
            finalInvokers.addAll(finalBitListInvokers);
        }

        if (finalInvokers == null) {
            finalInvokers = new ArrayList<>(invokers);
        }

        // 针对final invokers list，此时就可以再用普通的router执行后续的路由过滤
        for (Router router : routers) {
            finalInvokers = router.route(finalInvokers, url, invocation);
        }

        return finalInvokers;
    }

    /**
     * Notify router chain of the initial addresses from registry at the first time.
     * Notify whenever addresses in registry change.
     */
    public void setInvokers(List<Invoker<T>> invokers) {
        this.invokers = (invokers == null ? Collections.emptyList() : invokers);
        stateRouters.forEach(router -> router.notify(this.invokers)); // 如果invokers list有变动
        // 遍历每一个state router，state router都会通知一下invokers list的变动，notify操作
        routers.forEach(router -> router.notify(this.invokers)); // 普通的router也会做一个notify
        loop(true); // loop循环操作
    }

    /**
     * Build the asynchronous address cache for stateRouter.
     * @param notify Whether the addresses in registry have changed.
     */
    private void buildCache(boolean notify) {
        if (CollectionUtils.isEmpty(invokers)) {
            return;
        }

        // 在这里就会去进行cache的构建
        // state router的名称，name -> router cache
        // router cache，tag -> invokers bitlist的缓存构建关系
        AddrCache<T> origin = cache.get();
        List<Invoker<T>> copyInvokers = new ArrayList<>(this.invokers);
        AddrCache<T> newCache = new AddrCache<T>();

        // 这个东西，就是state router的name -> router cache的缓存映射关系
        Map<String, RouterCache<T>> routerCacheMap = new HashMap<>((int) (stateRouters.size() / 0.75f) + 1);

        newCache.setInvokers(invokers);

        for (StateRouter stateRouter : stateRouters) {
            try {
                // 对每个state router都会去构建一份router cache出来
                RouterCache routerCache = poolRouter(stateRouter, origin, copyInvokers, notify);
                //file cache 把state router的name和router cache缓存到一个map里去就可以了
                routerCacheMap.put(stateRouter.getName(), routerCache);
            } catch (Throwable t) {
                logger.error("Failed to pool router: " + stateRouter.getUrl() + ", cause: " + t.getMessage(), t);
                return;
            }
        }

        // 会把最新的router cache map放到addr cache里面去
        newCache.setCache(routerCacheMap);
        this.cache.set(newCache); // 还会把他设置到atomic reference里面去
    }

    /**
     * Cache the address list for each StateRouter.
     * @param router router
     * @param origin The original address cache
     * @param invokers The full address list
     * @param notify Whether the addresses in registry has changed.
     * @return
     */
    private RouterCache poolRouter(StateRouter router, AddrCache<T> origin, List<Invoker<T>> invokers, boolean notify) {
        String routerName = router.getName();
        RouterCache routerCache;
        if (isCacheMiss(origin, routerName) || router.shouldRePool() || notify) {
            return router.pool(invokers);
        } else {
            routerCache = origin.getCache().get(routerName);
        }
        if (routerCache == null) {
            return new RouterCache();
        }
        return routerCache;
    }

    private boolean isCacheMiss(AddrCache<T> cache, String routerName) {
        return cache == null || cache.getCache() == null || cache.getInvokers() == null || cache.getCache().get(
            routerName)
            == null;
    }

    /***
     * Build the asynchronous address cache for stateRouter. 构建一个异步化的address cache，是专门为state router去做的
     * @param notify Whether the addresses in registry has changed.
     */
    public void loop(boolean notify) {
        // 如果说是第一次来build cache，first build cache肯定是true
        // 此时做一个cas，就可以把true -> false
        // 如果是第二次，第三次，第四次，first build cache已经是false，cas一定是没法操作成功的
        if (firstBuildCache.compareAndSet(true,false)) {
            // 先构建一下对应的最新的state router的cache数据
            buildCache(notify);
        }

        try {
            if (notify) {
                // 基于信号量做一个并发的控制
                if (loopPermitNotify.tryAcquire()) {
                    loopPool.submit(new NotifyLoopRunnable(true, loopPermitNotify));
                }
            } else {
                if (loopPermit.tryAcquire()) {
                    loopPool.submit(new NotifyLoopRunnable(false, loopPermit));
                }
            }
        } catch (RejectedExecutionException e) {
            if (loopPool.isShutdown()){
                logger.warn("loopPool executor service is shutdown, ignoring notify loop");
                return;
            }
            throw e;
        }
    }

    class NotifyLoopRunnable implements Runnable {

        private final boolean notify;
        private final Semaphore loopPermit;

        public NotifyLoopRunnable(boolean notify, Semaphore loopPermit) {
            this.notify = notify;
            this.loopPermit = loopPermit;
        }

        @Override
        public void run() {
            // 在这里是异步化的去进行cache的构建
            buildCache(notify);
            loopPermit.release(); // 如果cache构建完毕了之后，就通过loop permit去做一个锁释放操作
        }
    }

    public void destroy() {
        invokers = Collections.emptyList();
        for (Router router : routers) {
            try {
                router.stop();
            } catch (Exception e) {
                logger.error("Error trying to stop router " + router.getClass(), e);
            }
        }
        routers = Collections.emptyList();
        builtinRouters = Collections.emptyList();

        for (StateRouter router : stateRouters) {
            try {
                router.stop();
            } catch (Exception e) {
                logger.error("Error trying to stop stateRouter " + router.getClass(), e);
            }
        }
        stateRouters = Collections.emptyList();
        builtinStateRouters = Collections.emptyList();
    }

}
