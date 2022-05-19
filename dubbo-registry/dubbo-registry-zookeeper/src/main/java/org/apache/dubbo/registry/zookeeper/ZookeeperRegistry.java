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
package org.apache.dubbo.registry.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.RegistryNotifier;
import org.apache.dubbo.registry.support.CacheableFailbackRegistry;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.zookeeper.ChildListener;
import org.apache.dubbo.remoting.zookeeper.StateListener;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;
import org.apache.dubbo.rpc.RpcException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.CHECK_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_SEPARATOR;
import static org.apache.dubbo.common.constants.RegistryConstants.CONFIGURATORS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONSUMERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.PROVIDERS_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.ROUTERS_CATEGORY;

/**
 * ZookeeperRegistry
 *
 * 基于zookeeper的注册中心组件
 *
 * 我们之前对注册中心这块的源码，很多细节都没去深挖的，尤其是服务订阅和发现这一块，类体系结构
 * 服务注册，zk里的目录结构，每个服务实例的url地址的深入研究，用的是zk里的持久化节点，还是临时节点
 * 包括就是说如果说provider/consumer自己突然宕机了以后怎么样
 * 如果zk突然之间了故障，此时会如何
 * provider/consumer跟zk之间的会话突然过期了，此时会如何
 *
 * 这种类体系，设计的极为极为的好，在面向对象的设计里
 * 值得大家好好去吸收和消化总结，大家不是一直想听why，dubbo注册中心的三级类体系结构设计（zk/nacos/redis具体注册中心技术 -> cacheable缓存层 -> failback故障重试层）
 * 通过父类体系的设计，把cache和failback两套复杂机制，提取到父类里去，而不是一个父类，是两个父类
 * 就可以把不同的机制模块进行一个拆分，cache和failback各自有一套机制模块
 * ZooKeeperRegistry、NacosRegistry、KubernetesRegistry、DnsRegistry、ConsulRegistry
 * 但是这些基于具体技术实现的注册中心，他们都可以继承相同的一套cache缓存和failback故障重试的机制，只要继承父类就可以了
 *
 * 类体系结构一共是5层，tech层、cacheable层、failback层、abstract层、interface层
 *
 */
public class ZookeeperRegistry extends CacheableFailbackRegistry {

    private final static Logger logger = LoggerFactory.getLogger(ZookeeperRegistry.class);

    private final static String DEFAULT_ROOT = "dubbo";

    // root，zk里面的根目录，/打头的第一级目录，/dubbo
    private final String root;
    // 用于保存对应的服务的string字符串的信息
    private final Set<String> anyServices = new ConcurrentHashSet<>();
    // 用来保存订阅时候施加的一个监听器，你对哪个服务施加监听，url -> 可能会有很多人施加监听
    // 一个map里会有两个listener，notify（dubbo里自己实现的监听器） -> child（针对zk的子节点施加的监听器）
    private final ConcurrentMap<URL, ConcurrentMap<NotifyListener, ChildListener>> zkListeners = new ConcurrentHashMap<>();
    // 跟zk建立网络连接的客户端
    private ZookeeperClient zkClient;

    /**
     * 刚开始构建这个zookeeper registry，核心的点，就是说去连接zk，跟zk建立连接就可以了
     * @param url
     * @param zookeeperTransporter
     */
    public ZookeeperRegistry(URL url, ZookeeperTransporter zookeeperTransporter) {
        // 传入进来的url是什么东西，必然说这种url就是zk的连接地址
        // zookeeper://localhost:2181/，拿着这个东西，先去走父类的构造函数
        super(url);

        if (url.isAnyHost()) {
            throw new IllegalStateException("registry address == null");
        }
        String group = url.getGroup(DEFAULT_ROOT);
        if (!group.startsWith(PATH_SEPARATOR)) {
            group = PATH_SEPARATOR + group;
        }
        this.root = group;
        // 基于zk的API，去构建跟zk之间的连接
        zkClient = zookeeperTransporter.connect(url);
        // 有一块关键的代码，之前并没有讲解到这块代码
        zkClient.addStateListener((state) -> {
            // 我们的客户端跟zk之间的连接，是有两个概念
            // 第一个概念，连接突然断开后，很快就进行了重新连接的操作，reconnected
            if (state == StateListener.RECONNECTED) {
                logger.warn("Trying to fetch the latest urls, in case there're provider changes during connection loss.\n" +
                    " Since ephemeral ZNode will not get deleted for a connection lose, " +
                    "there's no need to re-register url of this instance.");
                // 一旦重新连接了之后，就会尝试去重新拉取你之前订阅过的provider服务实例，最新的集群地址
                // 在你跟zk之间短暂的连接断开的时候，zk端的provider服务实例地址有变化，当时网络连接短暂断开了，导致么有办法当时反向推送给你
                // 所以此时你客户端，如果有短暂连接再重连的状态，必须去重新拉取一下最新的地址
                // 有没有在这个状态下去执行你的consumer节点的一个重新的注册呢？provider节点是否有必要再这里进行重新注册呢？
                // 重新发起订阅的请求，都是没有必要的
                // 假设你之前去进行注册，注册过去创建的就是一个zk的临时节点，如果你仅仅时突然的网络断开，他是不会删除你创建的zk临时节点的
                // 之前你发起的subsrbie订阅请求，在zk端，因为仅仅是临时的网络断开，所以zk也是不会删除你的订阅的监听的
                ZookeeperRegistry.this.fetchLatestAddresses();
            } else if (state == StateListener.NEW_SESSION_CREATED) {
                // new session created
                // 如果你要是经历了session断开以及会话重新连接的过程，新建立了一个会话的话
                // 需要重新进行注册和订阅的监听
                logger.warn("Trying to re-register urls and re-subscribe listeners of this instance to registry...");
                try {
                    ZookeeperRegistry.this.recover();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            } else if (state == StateListener.SESSION_LOST) {
                // 连接断开的时间超过了一定的阈值，session expire过期的时间
                // 会话一旦说过期了，此时zk服务端就会把我们的客户端之前注册创建的临时节点删除，同时施加的订阅监听也会删除
                // 我们的客户端收到的一个状态变更，那么就是session lost，断开时间太长了
                logger.warn("Url of this instance will be deleted from registry soon. " +
                    "Dubbo client will try to re-register once a new session is created.");
                // 在这里是不是可以去做一些处理呢？
                // 等待一段时间，如果一段时间后，还是没有自动出现new_session_created状态，此时就可以主动
                // 重新去定时尝试反复连接你的zk服务器端，在这里都可以自己去做一个实现
            } else if (state == StateListener.SUSPENDED) {

            } else if (state == StateListener.CONNECTED) {

            }
        });
    }

    @Override
    public boolean isAvailable() {
        return zkClient != null && zkClient.isConnected();
    }

    @Override
    public void destroy() {
        super.destroy();
        // Just release zkClient reference, but can not close zk client here for zk client is shared somewhere else.
        // See org.apache.dubbo.remoting.zookeeper.AbstractZookeeperTransporter#destroy()
        zkClient = null;
    }

    private void checkDestroyed() {
        if (zkClient == null) {
            throw new IllegalStateException("registry is destroyed");
        }
    }

    @Override
    public void doRegister(URL url) {
        // url到底了代表了什么，url代表了一个provider服务实例的所有的信息、配置和属性
        // 一个url就代表了一个provider服务实例

        try {
            // 真正核心的dubbo往zk进行服务实例注册的方法是在这里
            checkDestroyed();
            // 对于一个应用而言，他的注册无非就是说是，去创建zk里的一个znode而已
            zkClient.create(toUrlPath(url), url.getParameter(DYNAMIC_KEY, true));
        } catch (Throwable e) {
            throw new RpcException("Failed to register " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doUnregister(URL url) {
        try {
            checkDestroyed();
            zkClient.delete(toUrlPath(url));
        } catch (Throwable e) {
            throw new RpcException("Failed to unregister " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doSubscribe(final URL url, final NotifyListener listener) {
        try {
            checkDestroyed();

            // 当前的这个url里面的服务接口是谁
            if (ANY_VALUE.equals(url.getServiceInterface())) {
                String root = toRootPath();
                boolean check = url.getParameter(CHECK_KEY, false);
                ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.computeIfAbsent(url, k -> new ConcurrentHashMap<>());
                ChildListener zkListener = listeners.computeIfAbsent(listener, k -> (parentPath, currentChilds) -> {
                    for (String child : currentChilds) {
                        child = URL.decode(child);
                        if (!anyServices.contains(child)) {
                            anyServices.add(child);
                            subscribe(url.setPath(child).addParameters(INTERFACE_KEY, child,
                                Constants.CHECK_KEY, String.valueOf(check)), k);
                        }
                    }
                });
                zkClient.create(root, false);
                List<String> services = zkClient.addChildListener(root, zkListener);
                if (CollectionUtils.isNotEmpty(services)) {
                    for (String service : services) {
                        service = URL.decode(service);
                        anyServices.add(service);
                        subscribe(url.setPath(service).addParameters(INTERFACE_KEY, service,
                            Constants.CHECK_KEY, String.valueOf(check)), listener);
                    }
                }
            } else {
                // 正常来说都是针对指定的接口去执行监听
                CountDownLatch latch = new CountDownLatch(1);
                try {
                    List<URL> urls = new ArrayList<>();

                    for (String path : toCategoriesPath(url)) {
                        // 默认情况下就一个path，/dubbo/服务接口/providers，针对这个路径和地址去执行订阅和监听就可以了
                        // 首先，先拿到这个地址下面所有的子节点列表，就可以拿到这个服务当前所有的providers实例地址
                        // 再去监听你的providers下的子节点地址列表，如果有变动就可以notify通知你
                        ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.computeIfAbsent(url, k -> new ConcurrentHashMap<>());
                        ChildListener zkListener = listeners.computeIfAbsent(listener, k -> new RegistryChildListenerImpl(url, path, k, latch));
                        if (zkListener instanceof RegistryChildListenerImpl) {
                            ((RegistryChildListenerImpl) zkListener).setLatch(latch);
                        }
                        // 对/dubbo/服务接口/providers，做一个检查，存在就不做什么，不存在就会去创建出来
                        zkClient.create(path, false);
                        // 针对你要订阅和发现的那个服务节点，去加一个监听器，而且第一次加监听器，就会直接把你的子节点列表返回
                        // 这个子节点列表，就是指定的provider服务实例集群地址列表
                        List<String> children = zkClient.addChildListener(path, zkListener);
                        if (children != null) {
                            // 每个子节点都是一个url字符串，每个url字符串都代表了一个provider服务实例
                            // 此时就需要把你的每个url字符串都转为一个URL对象
                            urls.addAll(toUrlsWithEmpty(url, path, children));
                        }
                    }
                    // 第一个url其实是我们的zk地址，listener是订阅的时候传入进来的一个监听器
                    // urls：我们订阅的时候直接发现的服务接口的providers集群地址
                    notify(url, listener, urls);
                } finally {
                    // tells the listener to run only after the sync notification of main thread finishes.
                    latch.countDown();
                }
            }
        } catch (Throwable e) {
            throw new RpcException("Failed to subscribe " + url + " to zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public void doUnsubscribe(URL url, NotifyListener listener) {
        checkDestroyed();
        ConcurrentMap<NotifyListener, ChildListener> listeners = zkListeners.get(url);
        if (listeners != null) {
            ChildListener zkListener = listeners.remove(listener);
            if (zkListener != null) {
                if (ANY_VALUE.equals(url.getServiceInterface())) {
                    String root = toRootPath();
                    zkClient.removeChildListener(root, zkListener);
                } else {
                    for (String path : toCategoriesPath(url)) {
                        zkClient.removeChildListener(path, zkListener);
                    }
                }
            }

            if (listeners.isEmpty()) {
                zkListeners.remove(url);
            }
        }
    }

    @Override
    public List<URL> lookup(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("lookup url == null");
        }
        try {
            checkDestroyed();
            List<String> providers = new ArrayList<>();
            for (String path : toCategoriesPath(url)) {
                List<String> children = zkClient.getChildren(path);
                if (children != null) {
                    providers.addAll(children);
                }
            }
            return toUrlsWithoutEmpty(url, providers);
        } catch (Throwable e) {
            throw new RpcException("Failed to lookup " + url + " from zookeeper " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    private String toRootDir() {
        if (root.equals(PATH_SEPARATOR)) {
            return root;
        }
        return root + PATH_SEPARATOR;
    }

    private String toRootPath() {
        return root;
    }

    private String toServicePath(URL url) {
        String name = url.getServiceInterface();
        if (ANY_VALUE.equals(name)) {
            return toRootPath();
        }
        // 把root path拼接你的服务接口名称的path
        return toRootDir() + URL.encode(name);
    }

    private String[] toCategoriesPath(URL url) {
        String[] categories;
        if (ANY_VALUE.equals(url.getCategory())) {
            categories = new String[]{PROVIDERS_CATEGORY, CONSUMERS_CATEGORY, ROUTERS_CATEGORY, CONFIGURATORS_CATEGORY};
        } else {
            categories = url.getCategory(new String[]{DEFAULT_CATEGORY});
        }
        String[] paths = new String[categories.length];
        for (int i = 0; i < categories.length; i++) {
            paths[i] = toServicePath(url) + PATH_SEPARATOR + categories[i];
        }
        return paths;
    }

    private String toCategoryPath(URL url) {
        // 两个部分来组成，service path
        // category的一个概念，就是说你是providers还是consumers
        return toServicePath(url) + PATH_SEPARATOR + url.getCategory(DEFAULT_CATEGORY);
    }

    private String toUrlPath(URL url) {
        // zk里的path，引入一些概念
        // category，分类，类别，目录，把你的url转我一个category path，再去拼接你的url自己做一个编码字符串
        return toCategoryPath(url) + PATH_SEPARATOR + URL.encode(url.toFullString());
    }

    /**
     * When zookeeper connection recovered from a connection loss, it need to fetch the latest provider list.
     * re-register watcher is only a side effect and is not mandate.
     */
    private void fetchLatestAddresses() {
        // subscribe
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<URL, Set<NotifyListener>>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Fetching the latest urls of " + recoverSubscribed.keySet());
            }
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    // 在这里等于是要重新发起一次订阅和监听的机制，此时可以复用failback里面的重试机制就可以了
                    removeFailedSubscribed(url, listener);
                    addFailedSubscribed(url, listener);
                }
            }
        }
    }

    @Override
    protected boolean isMatch(URL subscribeUrl, URL providerUrl) {
        return UrlUtils.isMatch(subscribeUrl, providerUrl);
    }

    private class RegistryChildListenerImpl implements ChildListener {
        private RegistryNotifier notifier;
        private long lastExecuteTime;
        private volatile CountDownLatch latch;

        public RegistryChildListenerImpl(URL consumerUrl, String path, NotifyListener listener, CountDownLatch latch) {
            this.latch = latch;
            notifier = new RegistryNotifier(getUrl(), ZookeeperRegistry.this.getDelay()) {
                @Override
                public void notify(Object rawAddresses) {
                    long delayTime = getDelayTime();
                    if (delayTime <= 0) {
                        this.doNotify(rawAddresses);
                    } else {
                        long interval = delayTime - (System.currentTimeMillis() - lastExecuteTime);
                        if (interval > 0) {
                            try {
                                Thread.sleep(interval);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }
                        lastExecuteTime = System.currentTimeMillis();
                        this.doNotify(rawAddresses);
                    }
                }

                @Override
                protected void doNotify(Object rawAddresses) {
                    ZookeeperRegistry.this.notify(consumerUrl, listener, ZookeeperRegistry.this.toUrlsWithEmpty(consumerUrl, path, (List<String>) rawAddresses));
                }
            };
        }

        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void childChanged(String path, List<String> children) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.warn("Zookeeper children listener thread was interrupted unexpectedly, may cause race condition with the main thread.");
            }
            notifier.notify(children);
        }
    }
}
