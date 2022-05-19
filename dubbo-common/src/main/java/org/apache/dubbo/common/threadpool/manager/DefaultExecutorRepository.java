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
package org.apache.dubbo.common.threadpool.manager;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionAccessor;
import org.apache.dubbo.common.extension.ExtensionAccessorAware;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadlocal.NamedInternalThreadFactory;
import org.apache.dubbo.common.threadpool.ThreadPool;
import org.apache.dubbo.common.utils.ExecutorUtil;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.config.ConsumerConfig;
import org.apache.dubbo.config.ModuleConfig;
import org.apache.dubbo.config.ProviderConfig;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ModuleModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.CONSUMER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_EXPORT_THREAD_NUM;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_REFER_THREAD_NUM;
import static org.apache.dubbo.common.constants.CommonConstants.EXECUTOR_SERVICE_COMPONENT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.THREADS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.THREAD_NAME_KEY;

/**
 * Consider implementing {@code Licycle} to enable executors shutdown when the process stops.
 */
public class DefaultExecutorRepository implements ExecutorRepository, ExtensionAccessorAware, ScopeModelAware {
    private static final Logger logger = LoggerFactory.getLogger(DefaultExecutorRepository.class);

    private int DEFAULT_SCHEDULER_SIZE = Runtime.getRuntime().availableProcessors();

    private final ExecutorService sharedExecutor;
    private final ScheduledExecutorService sharedScheduledExecutor;

    // 此处的ring是一个取用环，用循环的概念不停的取用list里面的数据，重复循环的取用
    // 不是一个数据环，对一个大小有限的数据结构，重复不停的循环写入，写到尾部了，再从头部开始写入
    private Ring<ScheduledExecutorService> scheduledExecutors = new Ring<>();

    private volatile ScheduledExecutorService serviceExportExecutor;

    private volatile ExecutorService serviceReferExecutor;

    private ScheduledExecutorService reconnectScheduledExecutor;

    public Ring<ScheduledExecutorService> registryNotificationExecutorRing = new Ring<>();

    private Ring<ScheduledExecutorService> serviceDiscoveryAddressNotificationExecutorRing = new Ring<>();

    private ScheduledExecutorService metadataRetryExecutor;

    private ConcurrentMap<String, ConcurrentMap<Integer, ExecutorService>> data = new ConcurrentHashMap<>();

    private ExecutorService poolRouterExecutor;

    private Ring<ExecutorService> executorServiceRing = new Ring<ExecutorService>();

    private final Object LOCK = new Object();
    private ExtensionAccessor extensionAccessor;

    private ApplicationModel applicationModel;

    public DefaultExecutorRepository() {
        // 在构建他的时候，会有一个用于共享使用的线程池，NamedThreadFactory是干什么的，用来设定你的这个线程池里的线程的名称的前缀就可以了
        sharedExecutor = Executors.newCachedThreadPool(new NamedThreadFactory("Dubbo-shared-handler", true));
        // 共享的定时调度的线程池
        sharedScheduledExecutor = Executors.newScheduledThreadPool(8, new NamedThreadFactory("Dubbo-shared-scheduler", true));

        // 有多少个cpu核，此时就可以做一个cpu核数量的遍历
        for (int i = 0; i < DEFAULT_SCHEDULER_SIZE; i++) {
            // 通过遍历，会创建一个一个的scheduled executor service
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("Dubbo-framework-scheduler-" + i, true));
            scheduledExecutors.addItem(scheduler);

            // router chain里面搞到的线程池，是一个线程数量固定位1的，有队列排队的一个线程池
            executorServiceRing.addItem(new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(1024), new NamedInternalThreadFactory("Dubbo-state-router-loop-" + i, true)
                , new ThreadPoolExecutor.AbortPolicy()));
        }

//        reconnectScheduledExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Dubbo-reconnect-scheduler"));

        // pool router线程池
        poolRouterExecutor = new ThreadPoolExecutor(1, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1024),
            new NamedInternalThreadFactory("Dubbo-state-router-pool-router", true), new ThreadPoolExecutor.AbortPolicy());

        for (int i = 0; i < DEFAULT_SCHEDULER_SIZE; i++) {
            ScheduledExecutorService serviceDiscoveryAddressNotificationExecutor =
                Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Dubbo-SD-address-refresh-" + i));
            ScheduledExecutorService registryNotificationExecutor =
                Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Dubbo-registry-notification-" + i));

            serviceDiscoveryAddressNotificationExecutorRing.addItem(serviceDiscoveryAddressNotificationExecutor);
            registryNotificationExecutorRing.addItem(registryNotificationExecutor);
        }

        metadataRetryExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Dubbo-metadata-retry"));
    }

    /**
     * Get called when the server or client instance initiating.
     *
     * @param url
     * @return
     */
    public synchronized ExecutorService createExecutorIfAbsent(URL url) {
        // data缓存，在某个地方肯定会有一个代码触发，肯定会触发这个方法的执行

        Map<Integer, ExecutorService> executors = data.computeIfAbsent(EXECUTOR_SERVICE_COMPONENT_KEY, k -> new ConcurrentHashMap<>());
        // Consumer's executor is sharing globally, key=Integer.MAX_VALUE. Provider's executor is sharing by protocol.
        // 一旦第一次被触发，这里必然会提取出来provider端的端口号，20880
        Integer portKey = CONSUMER_SIDE.equalsIgnoreCase(url.getParameter(SIDE_KEY)) ? Integer.MAX_VALUE : url.getPort();
        if (url.getParameter(THREAD_NAME_KEY) == null) {
            url = url.putAttribute(THREAD_NAME_KEY, "Dubbo-protocol-"+portKey);
        }
        URL finalUrl = url;
        // 把你的20880作为一个key，同时创建出一个线程池出来
        ExecutorService executor = executors.computeIfAbsent(portKey, k -> createExecutor(finalUrl));
        // If executor has been shut down, create a new one
        if (executor.isShutdown() || executor.isTerminated()) {
            executors.remove(portKey);
            executor = createExecutor(url);
            executors.put(portKey, executor);
        }
        return executor;
    }

    public ExecutorService getExecutor(URL url) {
        // 先拿到一个固定的port->executor线程池之间的缓存
        Map<Integer, ExecutorService> executors = data.get(EXECUTOR_SERVICE_COMPONENT_KEY);

        /**
         * It's guaranteed that this method is called after {@link #createExecutorIfAbsent(URL)}, so data should already
         * have Executor instances generated and stored.
         */
        if (executors == null) {
            logger.warn("No available executors, this is not expected, framework should call createExecutorIfAbsent first " +
                "before coming to here.");
            return null;
        }

        // Consumer's executor is sharing globally, key=Integer.MAX_VALUE. Provider's executor is sharing by protocol.
        // 计算一个所谓的port key
        Integer portKey = CONSUMER_SIDE.equalsIgnoreCase(url.getParameter(SIDE_KEY)) ? Integer.MAX_VALUE : url.getPort();
        ExecutorService executor = executors.get(portKey);
        if (executor != null && (executor.isShutdown() || executor.isTerminated())) {
            executors.remove(portKey);
            // Does not re-create a shutdown executor, use SHARED_EXECUTOR for downgrade.
            executor = null;
            logger.info("Executor for " + url + " is shutdown.");
        }
        if (executor == null) {
            return sharedExecutor;
        } else {
            return executor;
        }
    }

    @Override
    public void updateThreadpool(URL url, ExecutorService executor) {
        try {
            if (url.hasParameter(THREADS_KEY)
                && executor instanceof ThreadPoolExecutor && !executor.isShutdown()) {
                ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executor;
                int threads = url.getParameter(THREADS_KEY, 0);
                int max = threadPoolExecutor.getMaximumPoolSize();
                int core = threadPoolExecutor.getCorePoolSize();
                if (threads > 0 && (threads != max || threads != core)) {
                    if (threads < core) {
                        threadPoolExecutor.setCorePoolSize(threads);
                        if (core == max) {
                            threadPoolExecutor.setMaximumPoolSize(threads);
                        }
                    } else {
                        threadPoolExecutor.setMaximumPoolSize(threads);
                        if (core == max) {
                            threadPoolExecutor.setCorePoolSize(threads);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            logger.error(t.getMessage(), t);
        }
    }

    @Override
    // 为什么线程池需要放在类似于ring的取用环，数据环，搞错了，看花眼了
    // 你不停的取用，取用，取用，取用的过程中，是在循环不断的取用不同的线程池
    public ScheduledExecutorService nextScheduledExecutor() {
        return scheduledExecutors.pollItem();
    }

    @Override
    public ExecutorService nextExecutorExecutor() {
        return executorServiceRing.pollItem();
    }

    @Override
    public ScheduledExecutorService getServiceExportExecutor() {
        if (serviceExportExecutor == null) {
            synchronized (LOCK) {
                if (serviceExportExecutor == null) {
                    int coreSize = getExportThreadNum();
                    serviceExportExecutor = Executors.newScheduledThreadPool(coreSize,
                        new NamedThreadFactory("Dubbo-service-export", true));
                }
            }
        }
        return serviceExportExecutor;
    }

    @Override
    public void shutdownServiceExportExecutor() {
        synchronized (LOCK) {
            if (serviceExportExecutor != null && !serviceExportExecutor.isShutdown()) {
                try {
                    serviceExportExecutor.shutdown();
                } catch (Throwable ignored) {
                    // ignored
                    logger.warn(ignored.getMessage(), ignored);
                }
            }
            serviceExportExecutor = null;
        }
    }

    @Override
    public ExecutorService getServiceReferExecutor() {
        if (serviceReferExecutor == null) {
            synchronized (LOCK) {
                if (serviceReferExecutor == null) {
                    int coreSize = getReferThreadNum();
                    serviceReferExecutor = Executors.newFixedThreadPool(coreSize,
                        new NamedThreadFactory("Dubbo-service-refer", true));
                }
            }
        }
        return serviceReferExecutor;
    }

    @Override
    public void shutdownServiceReferExecutor() {
        synchronized (LOCK) {
            if (serviceReferExecutor != null && !serviceReferExecutor.isShutdown()) {
                try {
                    serviceReferExecutor.shutdown();
                } catch (Throwable ignored) {
                    logger.warn(ignored.getMessage(), ignored);
                }
            }
            serviceReferExecutor = null;
        }
    }

    private Integer getExportThreadNum() {
        Integer threadNum = null;
        ApplicationModel applicationModel = ApplicationModel.ofNullable(this.applicationModel);
        for (ModuleModel moduleModel : applicationModel.getPubModuleModels()) {
            threadNum = getExportThreadNum(moduleModel);
            if (threadNum != null) {
                break;
            }
        }
        if (threadNum == null) {
            logger.info("Cannot get config `export-thread-num` from module config, using default: " + DEFAULT_EXPORT_THREAD_NUM);
            return DEFAULT_EXPORT_THREAD_NUM;
        }
        return threadNum;
    }

    private Integer getExportThreadNum(ModuleModel moduleModel) {
        ModuleConfig moduleConfig = moduleModel.getConfigManager().getModule().orElse(null);
        if (moduleConfig == null) {
            return null;
        }
        Integer threadNum = moduleConfig.getExportThreadNum();
        if (threadNum == null) {
            threadNum = moduleModel.getConfigManager().getProviders()
                .stream()
                .map(ProviderConfig::getExportThreadNum)
                .filter(k -> k != null && k > 0)
                .findAny().orElse(null);
        }
        return threadNum;
    }

    private Integer getReferThreadNum() {
        Integer threadNum = null;
        ApplicationModel applicationModel = ApplicationModel.ofNullable(this.applicationModel);
        for (ModuleModel moduleModel : applicationModel.getPubModuleModels()) {
            threadNum = getReferThreadNum(moduleModel);
            if (threadNum != null) {
                break;
            }
        }
        if (threadNum == null) {
            logger.info("Cannot get config `refer-thread-num` from module config, using default: " + DEFAULT_REFER_THREAD_NUM);
            return DEFAULT_REFER_THREAD_NUM;
        }
        return threadNum;
    }

    private Integer getReferThreadNum(ModuleModel moduleModel) {
        ModuleConfig moduleConfig = moduleModel.getConfigManager().getModule().orElse(null);
        if (moduleConfig == null) {
            return null;
        }
        Integer threadNum = moduleConfig.getReferThreadNum();
        if (threadNum == null) {
            threadNum = moduleModel.getConfigManager().getConsumers()
                .stream()
                .map(ConsumerConfig::getReferThreadNum)
                .filter(k -> k != null && k > 0)
                .findAny().orElse(null);
        }
        return threadNum;
    }

    @Override
    public ScheduledExecutorService getRegistryNotificationExecutor() {
        return registryNotificationExecutorRing.pollItem();
    }

    public ScheduledExecutorService getServiceDiscoveryAddressNotificationExecutor() {
        return serviceDiscoveryAddressNotificationExecutorRing.pollItem();
    }

    @Override
    public ScheduledExecutorService getMetadataRetryExecutor() {
        return metadataRetryExecutor;
    }

    @Override
    public ExecutorService getSharedExecutor() {
        return sharedExecutor;
    }

    @Override
    public ScheduledExecutorService getSharedScheduledExecutor() {
        return sharedScheduledExecutor;
    }

    private ExecutorService createExecutor(URL url) {
        // 也是用的SPI的机制
        // 讲过dubbo的业务线程池，有很多种类型，默认的就是fixed threadpool
        // 默认是fixed threadpool，adaptive自适应，他其实在真正获取线程池的时候，是会去根据url里的具体的参数来找到对应的实现类
        // adaptive，但是可以 看出来默认还是走fixed threadpool，默认的线程数量就是200个

        // SPI的adaptive自适应机制
        // 还是去生成代理类，代理类的getExecutor方法被调用，在里面必须通过url的参数，提取出来对应的线程池的短名称
        // 去找到对应的实现类的，再执行真正的getExecutor方法
        return (ExecutorService) extensionAccessor.getExtensionLoader(ThreadPool.class).getAdaptiveExtension().getExecutor(url);
    }

    @Override
    public ExecutorService getPoolRouterExecutor() {
        return poolRouterExecutor;
    }

    @Override
    public void destroyAll() {
        logger.info("destroying executor repository ..");
        shutdownExecutorService(poolRouterExecutor, "poolRouterExecutor");
        shutdownExecutorService(metadataRetryExecutor, "metadataRetryExecutor");

        shutdownServiceExportExecutor();
        shutdownServiceReferExecutor();

        data.values().forEach(executors -> {
            if (executors != null) {
                executors.values().forEach(executor -> {
                    if (executor != null && !executor.isShutdown()) {
                        try {
                            ExecutorUtil.shutdownNow(executor, 100);
                        } catch (Throwable ignored) {
                            // ignored
                            logger.warn(ignored.getMessage(), ignored);
                        }
                    }
                });
            }
        });
        data.clear();

        // scheduledExecutors
        shutdownExecutorServices(scheduledExecutors.listItems(), "scheduledExecutors");

        // executorServiceRing
        shutdownExecutorServices(executorServiceRing.listItems(), "executorServiceRing");

        // shutdown share executor
        shutdownExecutorService(sharedExecutor, "sharedExecutor");
        shutdownExecutorService(sharedScheduledExecutor, "sharedScheduledExecutor");

        // serviceDiscoveryAddressNotificationExecutorRing
        shutdownExecutorServices(serviceDiscoveryAddressNotificationExecutorRing.listItems(),
            "serviceDiscoveryAddressNotificationExecutorRing");

        // registryNotificationExecutorRing
        shutdownExecutorServices(registryNotificationExecutorRing.listItems(),
            "registryNotificationExecutorRing");

    }

    private void shutdownExecutorServices(List<? extends ExecutorService> executorServices, String msg) {
        for (ExecutorService executorService : executorServices) {
            shutdownExecutorService(executorService, msg);
        }
    }

    private void shutdownExecutorService(ExecutorService executorService, String name) {
        try {
            executorService.shutdownNow();
        } catch (Exception e) {
            String msg = "shutdown executor service [" + name + "] failed: ";
            logger.warn(msg + e.getMessage(), e);
        }
    }

    @Override
    public void setExtensionAccessor(ExtensionAccessor extensionAccessor) {
        this.extensionAccessor = extensionAccessor;
    }

    @Override
    public void setApplicationModel(ApplicationModel applicationModel) {
        this.applicationModel = applicationModel;
    }
}
