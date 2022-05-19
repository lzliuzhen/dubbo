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
package org.apache.dubbo.registry.dns;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.registry.client.DefaultServiceInstance;
import org.apache.dubbo.registry.client.SelfHostMetaServiceDiscovery;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.event.listener.ServiceInstancesChangedListener;
import org.apache.dubbo.registry.dns.util.DNSClientConst;
import org.apache.dubbo.registry.dns.util.DNSResolver;
import org.apache.dubbo.registry.dns.util.ResolveResult;
import org.apache.dubbo.rpc.model.ScopeModelUtil;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

// dubbo服务，一般来说都是基于zk和nacos作为注册中心的，他原生的dubbo服务，不会去针对dns去做注册
// 所以说，对于dubbo服务而言，没有必要采用dns作为注册中心，registry是空的
// 但是，dubbo服务需要从dns里发现其他的一个服务，k8s服务假设注册到了dns里去了，此时你要发现k8s服务
// 你就必须支持从dns里进行服务发现
// 发现了服务以后，此时你就可以根据ip地质去访问异构技术的其他服务了
public class DNSServiceDiscovery extends SelfHostMetaServiceDiscovery {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * DNS properties
     */

    private String addressPrefix;
    private String addressSuffix;
    private long pollingCycle;
    private DNSResolver dnsResolver;

    /**
     * Polling task ScheduledFuture, used to stop task when destroy
     */
    private final ConcurrentHashMap<String, ScheduledFuture<?>> pollingExecutorMap = new ConcurrentHashMap<>();

    /**
     * Polling check provider ExecutorService
     */
    private ScheduledExecutorService pollingExecutorService;

    @Override
    public void doInitialize(URL registryURL) throws Exception {
        this.addressPrefix = registryURL.getParameter(DNSClientConst.ADDRESS_PREFIX, "");
        this.addressSuffix = registryURL.getParameter(DNSClientConst.ADDRESS_SUFFIX, "");
        // polling cycle，多长时间做一次主动的轮询和拉取
        this.pollingCycle = registryURL.getParameter(DNSClientConst.DNS_POLLING_CYCLE, DNSClientConst.DEFAULT_DNS_POLLING_CYCLE);

        // 注册url的地址，针对的就是dns的地址，从这个里面拿出来的host，取名字叫做name server，命名服务器，dns服务器
        String nameserver = registryURL.getHost();
        int port = registryURL.getPort();
        // 每一次服务名称的解析，最多可以查询多少次，resolve解析，域名解析，服务名称解析，默认是最多10次
        int maxQueriesPerResolve = registryURL.getParameter(DNSClientConst.MAX_QUERIES_PER_RESOLVE, 10);
        // 封装一个DNS解析组件，nameserver（dns地址）、port（dns端口号）、每一次服务解析最多可以查询（10次）
        this.dnsResolver = new DNSResolver(nameserver, port, maxQueriesPerResolve);

        // 调度线程池的线程数量，默认是1个
        int scheduledThreadPoolSize = registryURL.getParameter(DNSClientConst.DNS_POLLING_POOL_SIZE_KEY, DNSClientConst.DEFAULT_DNS_POLLING_POOL_SIZE);

        // polling task may take a lot of time, create a new ScheduledThreadPool
        // 默认创建了一个polling线程池，默认就1个线程
        pollingExecutorService = Executors.newScheduledThreadPool(scheduledThreadPoolSize, new NamedThreadFactory("Dubbo-DNS-Poll"));
    }

    @Override
    public void doDestroy() throws Exception {
        dnsResolver.destroy();
        pollingExecutorMap.forEach((serviceName, scheduledFuture) -> scheduledFuture.cancel(true));
        pollingExecutorMap.clear();
        pollingExecutorService.shutdown();
    }

    @Override
    public Set<String> getServices() {
        // it is impossible for dns to discover service names
        return Collections.singleton("Unsupported Method");
    }

    @Override
    public List<ServiceInstance> getInstances(String serviceName) throws NullPointerException {
        // 你的服务名称拼接上前缀和后缀
        String serviceAddress = addressPrefix + serviceName + addressSuffix;
        // 最最主要的，其实就是在这里基于DNSResolver去做一个地址解析，针对你的服务名称的地址，解析获取他的ip地址列表
        ResolveResult resolveResult = dnsResolver.resolve(serviceAddress);
        return toServiceInstance(serviceName, resolveResult);
    }

    @Override
    public void addServiceInstancesChangedListener(ServiceInstancesChangedListener listener) throws NullPointerException, IllegalArgumentException {
        listener.getServiceNames().forEach(serviceName -> {
            ScheduledFuture<?> scheduledFuture = pollingExecutorService.scheduleAtFixedRate(() -> {
                        List<ServiceInstance> instances = getInstances(serviceName);
                        instances.sort(Comparator.comparingInt(ServiceInstance::hashCode));
                        notifyListener(serviceName, listener, instances);
                    },
                    pollingCycle, pollingCycle, TimeUnit.MILLISECONDS);

            pollingExecutorMap.put(serviceName, scheduledFuture);
        });
    }

    /**
     * UT used only
     */
    @Deprecated
    public void setDnsResolver(DNSResolver dnsResolver) {
        this.dnsResolver = dnsResolver;
    }

    private List<ServiceInstance> toServiceInstance(String serviceName, ResolveResult resolveResult) {

        int port;

        if (resolveResult.getPort().size() > 0) {
            // use first as default
            port = resolveResult.getPort().get(0);
        } else {
            // not support SRV record
            port = 20880;
        }

        List<ServiceInstance> instanceList = new LinkedList<>();

        for (String host : resolveResult.getHostnameList()) {
            DefaultServiceInstance serviceInstance = new DefaultServiceInstance(serviceName, host, port, ScopeModelUtil.getApplicationModel(getUrl().getScopeModel()));
            fillServiceInstance(serviceInstance);
            instanceList.add(serviceInstance);
        }

        return instanceList;
    }
}
