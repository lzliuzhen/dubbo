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
package org.apache.dubbo.registry.integration;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.registry.AddressListener;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.registry.client.migration.InvokersChangedListener;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Cluster;
import org.apache.dubbo.rpc.cluster.Configurator;
import org.apache.dubbo.rpc.cluster.RouterChain;
import org.apache.dubbo.rpc.cluster.RouterFactory;
import org.apache.dubbo.rpc.cluster.directory.AbstractDirectory;

import java.util.Collections;
import java.util.List;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.CONSUMERS_CATEGORY;
import static org.apache.dubbo.registry.Constants.REGISTER_KEY;
import static org.apache.dubbo.registry.Constants.SIMPLIFIED_KEY;
import static org.apache.dubbo.registry.integration.InterfaceCompatibleRegistryProtocol.DEFAULT_REGISTER_CONSUMER_KEYS;
import static org.apache.dubbo.remoting.Constants.CHECK_KEY;


/**
 * RegistryDirectory
 *
 * 细节的源码分析，那么就全部都是说用静态代码来分析了，除非是特殊意外，可能会用动态调试来分析
 * DynamicDirectory = RegistryDirectory，2.7.x，2.6.x源码
 * 他是最核心的用于进行服务发现的一个组件，consumer端肯定是要去调用一个provider端的
 * 动态目录，动态可变的一个目标服务实例的集群地址（invokers）
 * 
 * 目标服务实例集群，不光是说要通过主动查询zk来第一次获取集群地址，还需要zk反过来，如果目标服务实例集群地址有变化
 * zk会感受到了，zk应该是会反过来推送你的目标服务实例集群地址给DynamicDirectory，动态更新和维护
 * 自己内存里的目标服务实例集群地址列表，invokers
 *
 */
public abstract class DynamicDirectory<T> extends AbstractDirectory<T> implements NotifyListener {

    private static final Logger logger = LoggerFactory.getLogger(DynamicDirectory.class);

    protected final Cluster cluster;

    protected final RouterFactory routerFactory;

    /**
     * Initialization at construction time, assertion not null
     */
    protected final String serviceKey;

    /**
     * Initialization at construction time, assertion not null
     */
    protected final Class<T> serviceType;

    /**
     * Initialization at construction time, assertion not null, and always assign non null value
     */
    protected final URL directoryUrl;
    protected final boolean multiGroup;

    /**
     * Initialization at the time of injection, the assertion is not null
     */
    protected Protocol protocol;

    /**
     * Initialization at the time of injection, the assertion is not null
     */
    protected Registry registry;
    protected volatile boolean forbidden = false;
    protected boolean shouldRegister;
    protected boolean shouldSimplified;

    /**
     * Initialization at construction time, assertion not null, and always assign not null value
     */
    protected volatile URL overrideDirectoryUrl;
    protected volatile URL subscribeUrl;
    protected volatile URL registeredConsumerUrl;

    /**
     * The initial value is null and the midway may be assigned to null, please use the local variable reference
     * override rules
     * Priority: override>-D>consumer>provider
     * Rule one: for a certain provider <ip:port,timeout=100>
     * Rule two: for all providers <* ,timeout=5000>
     */
    protected volatile List<Configurator> configurators;

    protected volatile List<Invoker<T>> invokers;

    protected ServiceInstancesChangedListener serviceListener;

    public DynamicDirectory(Class<T> serviceType, URL url) {
        super(url, true);

        this.cluster = url.getOrDefaultApplicationModel().getExtensionLoader(Cluster.class).getAdaptiveExtension();
        this.routerFactory = url.getOrDefaultApplicationModel().getExtensionLoader(RouterFactory.class).getAdaptiveExtension();

        if (serviceType == null) {
            throw new IllegalArgumentException("service type is null.");
        }

        if (url.getServiceKey() == null || url.getServiceKey().length() == 0) {
            throw new IllegalArgumentException("registry serviceKey is null.");
        }

        this.shouldRegister = !ANY_VALUE.equals(url.getServiceInterface()) && url.getParameter(REGISTER_KEY, true);
        this.shouldSimplified = url.getParameter(SIMPLIFIED_KEY, false);

        this.serviceType = serviceType;
        this.serviceKey = super.getConsumerUrl().getServiceKey();

        this.overrideDirectoryUrl = this.directoryUrl = consumerUrl;
        String group = directoryUrl.getGroup("");
        this.multiGroup = group != null && (ANY_VALUE.equals(group) || group.contains(","));
    }

    @Override
    public void addServiceListener(ServiceInstancesChangedListener instanceListener) {
        this.serviceListener = instanceListener;
    }

    public void setProtocol(Protocol protocol) {
        this.protocol = protocol;
    }

    public void setRegistry(Registry registry) {
        this.registry = registry;
    }

    public Registry getRegistry() {
        return registry;
    }

    public boolean isShouldRegister() {
        return shouldRegister;
    }

    public void subscribe(URL url) {
        setSubscribeUrl(url);
        // 在这里是一个关键的方法，实现服务发现靠的就是这个方法
        registry.subscribe(url, this);
    }

    public void unSubscribe(URL url) {
        setSubscribeUrl(null);
        registry.unsubscribe(url, this);
    }

    @Override
    public List<Invoker<T>> doList(Invocation invocation) {
        if (forbidden) {
            // 1. No service provider 2. Service providers are disabled
            throw new RpcException(RpcException.FORBIDDEN_EXCEPTION, "No provider available from registry " +
                getUrl().getAddress() + " for service " + getConsumerUrl().getServiceKey() + " on consumer " +
                NetUtils.getLocalHost() + " use dubbo version " + Version.getVersion() +
                ", please check status of providers(disabled, not registered or in blacklist).");
        }

        if (multiGroup) {
            return this.invokers == null ? Collections.emptyList() : this.invokers;
        }

        // 定义了一个invokers集合
        List<Invoker<T>> invokers = null;
        try {
            // Get invokers from cache, only runtime routers will be executed.
            invokers = routerChain.route(getConsumerUrl(), invocation);
        } catch (Throwable t) {
            logger.error("Failed to execute router: " + getUrl() + ", cause: " + t.getMessage(), t);
        }

        return invokers == null ? Collections.emptyList() : invokers;
    }

    @Override
    public Class<T> getInterface() {
        return serviceType;
    }

    @Override
    public List<Invoker<T>> getAllInvokers() {
        return this.invokers == null ? Collections.emptyList() : this.invokers;
    }

    /**
     * The currently effective consumer url
     *
     * @return URL
     */
    @Override
    public URL getConsumerUrl() {
        return this.overrideDirectoryUrl;
    }

    /**
     * The original consumer url
     *
     * @return URL
     */
    public URL getOriginalConsumerUrl() {
        return this.overrideDirectoryUrl;
    }

    /**
     * The url registered to registry or metadata center
     *
     * @return URL
     */
    public URL getRegisteredConsumerUrl() {
        return registeredConsumerUrl;
    }

    /**
     * The url used to subscribe from registry
     *
     * @return URL
     */
    public URL getSubscribeUrl() {
        return subscribeUrl;
    }

    public void setSubscribeUrl(URL subscribeUrl) {
        this.subscribeUrl = subscribeUrl;
    }

    public void setRegisteredConsumerUrl(URL url) {
        if (!shouldSimplified) {
            this.registeredConsumerUrl = url.addParameters(CATEGORY_KEY, CONSUMERS_CATEGORY, CHECK_KEY,
                String.valueOf(false));
        } else {
            this.registeredConsumerUrl = URL.valueOf(url, DEFAULT_REGISTER_CONSUMER_KEYS, null).addParameters(
                CATEGORY_KEY, CONSUMERS_CATEGORY, CHECK_KEY, String.valueOf(false));
        }
    }

    public void buildRouterChain(URL url) {
        this.setRouterChain(RouterChain.buildChain(url));
    }

    public List<Invoker<T>> getInvokers() {
        return invokers;
    }

    @Override
    public void destroy() {
        if (isDestroyed()) {
            return;
        }

        // unregister.
        try {
            if (getRegisteredConsumerUrl() != null && registry != null && registry.isAvailable()) {
                registry.unregister(getRegisteredConsumerUrl());
            }
        } catch (Throwable t) {
            logger.warn("unexpected error when unregister service " + serviceKey + " from registry: " + registry.getUrl(), t);
        }
        // unsubscribe.
        try {
            if (getSubscribeUrl() != null && registry != null && registry.isAvailable()) {
                registry.unsubscribe(getSubscribeUrl(), this);
            }
        } catch (Throwable t) {
            logger.warn("unexpected error when unsubscribe service " + serviceKey + " from registry: " + registry.getUrl(), t);
        }

        ExtensionLoader<AddressListener> addressListenerExtensionLoader = getUrl().getOrDefaultModuleModel().getExtensionLoader(AddressListener.class);
        List<AddressListener> supportedListeners = addressListenerExtensionLoader.getActivateExtension(getUrl(), (String[]) null);
        if (supportedListeners != null && !supportedListeners.isEmpty()) {
            for (AddressListener addressListener : supportedListeners) {
                addressListener.destroy(getConsumerUrl(), this);
            }
        }

        synchronized (this) {
            try {
                destroyAllInvokers();
            } catch (Throwable t) {
                logger.warn("Failed to destroy service " + serviceKey, t);
            }
            routerChain.destroy();
            invokersChangedListener = null;
            serviceListener = null;

            super.destroy(); // must be executed after unsubscribing
        }
    }

    @Override
    public void discordAddresses() {
        try {
            destroyAllInvokers();
        } catch (Throwable t) {
            logger.warn("Failed to destroy service " + serviceKey, t);
        }
    }

    private volatile InvokersChangedListener invokersChangedListener;
    private volatile boolean invokersChanged;

    public synchronized void setInvokersChangedListener(InvokersChangedListener listener) {
        this.invokersChangedListener = listener;
        if (invokersChangedListener != null && invokersChanged) {
            invokersChangedListener.onChange();
        }
    }

    protected synchronized void invokersChanged() {
        invokersChanged = true;
        if (invokersChangedListener != null) {
            invokersChangedListener.onChange();
            invokersChanged = false;
        }
    }

    @Override
    public boolean isNotificationReceived() {
        return invokersChanged;
    }

    protected abstract void destroyAllInvokers();
}
