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
package org.apache.dubbo.config.deploy;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.config.Environment;
import org.apache.dubbo.common.config.ReferenceCache;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.config.configcenter.DynamicConfigurationFactory;
import org.apache.dubbo.common.config.configcenter.wrapper.CompositeDynamicConfiguration;
import org.apache.dubbo.common.deploy.AbstractDeployer;
import org.apache.dubbo.common.deploy.ApplicationDeployListener;
import org.apache.dubbo.common.deploy.ApplicationDeployer;
import org.apache.dubbo.common.deploy.DeployState;
import org.apache.dubbo.common.deploy.ModuleDeployer;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.lang.ShutdownHookCallbacks;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.ConfigCenterConfig;
import org.apache.dubbo.config.DubboShutdownHook;
import org.apache.dubbo.config.MetadataReportConfig;
import org.apache.dubbo.config.RegistryConfig;
import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.config.utils.CompositeReferenceCache;
import org.apache.dubbo.config.utils.ConfigValidationUtils;
import org.apache.dubbo.metadata.MetadataService;
import org.apache.dubbo.metadata.MetadataServiceExporter;
import org.apache.dubbo.metadata.WritableMetadataService;
import org.apache.dubbo.metadata.report.MetadataReportFactory;
import org.apache.dubbo.metadata.report.MetadataReportInstance;
import org.apache.dubbo.registry.client.DefaultServiceInstance;
import org.apache.dubbo.registry.client.ServiceInstance;
import org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils;
import org.apache.dubbo.registry.client.metadata.store.InMemoryWritableMetadataService;
import org.apache.dubbo.registry.client.metadata.store.RemoteMetadataServiceImpl;
import org.apache.dubbo.registry.support.RegistryManager;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ModuleModel;
import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static java.lang.String.format;
import static org.apache.dubbo.common.config.ConfigurationUtils.parseProperties;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.constants.CommonConstants.REGISTRY_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.utils.StringUtils.isEmpty;
import static org.apache.dubbo.common.utils.StringUtils.isNotEmpty;
import static org.apache.dubbo.metadata.MetadataConstants.DEFAULT_METADATA_PUBLISH_DELAY;
import static org.apache.dubbo.metadata.MetadataConstants.METADATA_PUBLISH_DELAY_KEY;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.calInstanceRevision;
import static org.apache.dubbo.registry.client.metadata.ServiceInstanceMetadataUtils.setMetadataStorageType;
import static org.apache.dubbo.remoting.Constants.CLIENT_KEY;

/**
 * initialize and start application instance
 */
public class DefaultApplicationDeployer extends AbstractDeployer<ApplicationModel> implements ApplicationDeployer {

    private static final Logger logger = LoggerFactory.getLogger(DefaultApplicationDeployer.class);

    private final ApplicationModel applicationModel;

    private final ConfigManager configManager;

    private final Environment environment;

    private final ReferenceCache referenceCache;

    private final ExecutorRepository executorRepository;

    private volatile ServiceInstance serviceInstance;

    private AtomicBoolean hasPreparedApplicationInstance = new AtomicBoolean(false);

    private volatile MetadataService metadataService;

    private volatile MetadataServiceExporter metadataServiceExporter;

    private ScheduledFuture<?> asyncMetadataFuture;
    private String identifier;
    private volatile CompletableFuture startFuture;
    private DubboShutdownHook dubboShutdownHook;
    private Object startedLock = new Object();

    public DefaultApplicationDeployer(ApplicationModel applicationModel) {
        super(applicationModel);
        this.applicationModel = applicationModel;
        configManager = applicationModel.getApplicationConfigManager();
        environment = applicationModel.getModelEnvironment();

        referenceCache = new CompositeReferenceCache(applicationModel);
        executorRepository = getExtensionLoader(ExecutorRepository.class).getDefaultExtension();
        dubboShutdownHook = new DubboShutdownHook(applicationModel);

        // load spi listener
        Set<ApplicationDeployListener> deployListeners = applicationModel.getExtensionLoader(ApplicationDeployListener.class)
            .getSupportedExtensionInstances();
        for (ApplicationDeployListener listener : deployListeners) {
            this.addDeployListener(listener);
        }
    }

    public static ApplicationDeployer get(ScopeModel moduleOrApplicationModel) {
        ApplicationModel applicationModel = ScopeModelUtil.getApplicationModel(moduleOrApplicationModel);
        ApplicationDeployer applicationDeployer = applicationModel.getDeployer();
        if (applicationDeployer == null) {
            applicationDeployer = applicationModel.getBeanFactory().getOrRegisterBean(DefaultApplicationDeployer.class);
        }
        return applicationDeployer;
    }

    @Override
    public ApplicationModel getApplicationModel() {
        return applicationModel;
    }

    private <T> ExtensionLoader<T> getExtensionLoader(Class<T> type) {
        return applicationModel.getExtensionLoader(type);
    }

    private void unRegisterShutdownHook() {
        dubboShutdownHook.unregister();
    }

    private boolean isRegisterConsumerInstance() {
        Boolean registerConsumer = getApplication().getRegisterConsumer();
        return Boolean.TRUE.equals(registerConsumer);
    }

    private String getMetadataType() {
        String type = getApplication().getMetadataType();
        if (StringUtils.isEmpty(type)) {
            type = DEFAULT_METADATA_STORAGE_TYPE;
        }
        return type;
    }

    @Override
    public ReferenceCache getReferenceCache() {
        return referenceCache;
    }

    /**
     * Initialize
     */
    @Override
    public void initialize() {
        if (initialized.get()) {
            return;
        }
        // Ensure that the initialization is completed when concurrent calls

        // 这里的源码是什么意思呢，就是你的ApplicationDeployer组件，可能会被多线程并发访问
        // 不一定说多个线程都是并发再访问你的这个initialize这个方法
        // 肯定会有一个线程是在执行你的initialize这个方法的，此时可能有别的线程会去访问别的方法
        // 别的线程访问别的方法，也可以用synchronized(this)加锁，必须被卡住
        // 等待你的这个线程把initialize过程先执行完毕了再说

        synchronized (this) {
            if (initialized.get()) {
                return;
            }
            // register shutdown hook
            registerShutdownHook();

            // 这些东西，其实就是deployer组件最关键的要干的事情

            // 老的dubbo版本，注册中心的概念
            // 后来随着版本的迭代和演进，出现了配置中心、元数据中心
            // 解耦的概念
            // dubbo的服务实例信息、配置信息、元数据信息，你都放在zk里也是可以的
            // 如果你把一个服务实例的各种数据和信息，都存储再zk里面，各种数据和信息是严重耦合在一起的
            // 扩展性和可用性，两个层面来分析一下，如果说你把各种数据都耦合在一起，都放zk里

            // 扩展性，服务实例的数据，在一个大规模的微服务系统里面，大厂里面，服务本身可能都有上百个，几十个
            // 服务实例可能有几百个，几千个，数据自己本身可能就很多
            // 配置数据，可能说，不是太多，元数据，可能也不是怎么太多，扩展性，数据扩展
            // 服务实例数据在膨胀，配置数据和元数据可能也在增加，但是线性膨胀的可能就是服务实例数据
            // 可能就需要对注册中心去进行一个扩容，可能是更换一个系统去存储，在这个过程之中，耦合在一起的，就动数据
            // 牵一发而动全身，这个就是所谓的多种不同门类的数据耦合在一起的痛点

            // 可用性，注册数据、配置数据、元数据，把它们三个都放在一个地方，zk
            // 此时可能会出现一个问题，万一说zk要是故障了，三种数据一起没了，可用性问题，耦合的问题在里面

            // dubbo现在最新的版本的架构设计思想，不同分类的数据进行解耦，service config和reference config
            // 两种角色分离设计的思想的时候，解耦，解耦，解耦
            // 把三种数据，注册数据、元数据、配置数据，三大数据可以做一个分离和解耦
            // 三大中心也做一个分离和解耦，注册中心、配置中心、元数据中心，我们就可以把三种不同门类的数据，放到不同的地方去

            // 数据扩展性这块，比如说你要对服务实例数据太多了，此时要扩容或者切换存储技术，对你的另外两种数据是没有直接的影响的
            // 可用性这块，也有一个增强，万一说zk作为注册中心突然挂了，此时配置中心假设可能是nacos，对他们来说，没有直接的影响这样子

            // 启动配置中心，如果说你要是启动了外部配置中心，此时就可以从外部配置中心加载你的全局配置属性
            // 后续你的router这一块，他们都可以通过配置中心监听他们对应的router路由配置节点
            // 有最新的router治理规则变更，就可以配置中心推送给他们了
            startConfigCenter();
            loadApplicationConfigs();
            initModuleDeployers();
            // @since 2.7.8
            startMetadataCenter();
            initMetadataService();

            initialized.set(true);

            if (logger.isInfoEnabled()) {
                logger.info(getIdentifier() + " has been initialized!");
            }
        }
    }

    private void registerShutdownHook() {
        dubboShutdownHook.register();
    }

    private void initModuleDeployers() {
        // make sure created default module
        applicationModel.getDefaultModule();
        for (ModuleModel moduleModel : applicationModel.getModuleModels()) {
            moduleModel.getDeployer().initialize();
        }
    }

    private void loadApplicationConfigs() {
        configManager.loadConfigs();
    }

    // ConfigCenter，配置中心连接apollo、nacos、zookeeper别的什么东西
    // dynamic configuration，动态化的配置
    private void startConfigCenter() {

        // load application config
        configManager.loadConfigsOfTypeFromProps(ApplicationConfig.class);

        // load config centers
        configManager.loadConfigsOfTypeFromProps(ConfigCenterConfig.class);

        // 如果有必要的话，直接使用注册中心作为自己的配置中心
        useRegistryAsConfigCenterIfNecessary();

        // check Config Center
        Collection<ConfigCenterConfig> configCenters = configManager.getConfigCenters();
        if (CollectionUtils.isEmpty(configCenters)) {
            ConfigCenterConfig configCenterConfig = new ConfigCenterConfig();
            configCenterConfig.setScopeModel(applicationModel);
            configCenterConfig.refresh();
            ConfigValidationUtils.validateConfigCenterConfig(configCenterConfig);
            if (configCenterConfig.isValid()) {
                configManager.addConfigCenter(configCenterConfig);
                configCenters = configManager.getConfigCenters();
            }
        } else {
            for (ConfigCenterConfig configCenterConfig : configCenters) {
                configCenterConfig.refresh();
                ConfigValidationUtils.validateConfigCenterConfig(configCenterConfig);
            }
        }

        if (CollectionUtils.isNotEmpty(configCenters)) {
            CompositeDynamicConfiguration compositeDynamicConfiguration = new CompositeDynamicConfiguration();
            for (ConfigCenterConfig configCenter : configCenters) {
                // Pass config from ConfigCenterBean to environment
                environment.updateExternalConfigMap(configCenter.getExternalConfiguration());
                environment.updateAppExternalConfigMap(configCenter.getAppExternalConfiguration());

                // Fetch config from remote config center
                compositeDynamicConfiguration.addConfiguration(prepareEnvironment(configCenter));
            }
            environment.setDynamicConfiguration(compositeDynamicConfiguration);
        }

        configManager.refreshAll();
    }

    private void startMetadataCenter() {

        // 第一步，先分析一下元数据中心，他是否要用注册中心当做元数据中心
        useRegistryAsMetadataCenterIfNecessary();

        ApplicationConfig applicationConfig = getApplication();

        // 在这个里面获取到一个metadata type，元数据类型
        String metadataType = applicationConfig.getMetadataType();
        // FIXME, multiple metadata config support.
        // 在这里我们做了一个元数据的配置，里面包含了我们一开始搞的那个东西，metadata report上报的配置
        Collection<MetadataReportConfig> metadataReportConfigs = configManager.getMetadataConfigs();
        if (CollectionUtils.isEmpty(metadataReportConfigs)) {
            if (REMOTE_METADATA_STORAGE_TYPE.equals(metadataType)) {
                throw new IllegalStateException("No MetadataConfig found, Metadata Center address is required when 'metadata=remote' is enabled.");
            }
            return;
        }

        // 就可以往后走了，app model拿到一个bean容器，从bean容器里拿到了一个MetadataReportInstance
        // 一看这个东西，就属于我们的元数据上报组件的实例
        MetadataReportInstance metadataReportInstance = applicationModel.getBeanFactory().getBean(MetadataReportInstance.class);

        for (MetadataReportConfig metadataReportConfig : metadataReportConfigs) {
            ConfigValidationUtils.validateMetadataConfig(metadataReportConfig);
            if (!metadataReportConfig.isValid()) {
                logger.info("Ignore invalid metadata-report config: " + metadataReportConfig);
                continue;
            }
            // 对于我们唯一的一个metadata report，在这里他就可以去做一个初始化
            // 把我们的配置的metadata report的地址，config，传入进去，进行一个init初始化
            // 所谓的metadata report的启动，无非就是说，根据我们的一个配置，拿到对应的factory
            // 通过factory创建出来我们对应的技术metadata report
            metadataReportInstance.init(metadataReportConfig);
        }
    }

    /**
     * For compatibility purpose, use registry as the default config center when
     * there's no config center specified explicitly and
     * useAsConfigCenter of registryConfig is null or true
     */
    private void useRegistryAsConfigCenterIfNecessary() {
        // we use the loading status of DynamicConfiguration to decide whether ConfigCenter has been initiated.
        if (environment.getDynamicConfiguration().isPresent()) {
            return;
        }

        if (CollectionUtils.isNotEmpty(configManager.getConfigCenters())) {
            return;
        }

        // load registry
        configManager.loadConfigsOfTypeFromProps(RegistryConfig.class);

        List<RegistryConfig> defaultRegistries = configManager.getDefaultRegistries();
        if (defaultRegistries.size() > 0) {
            defaultRegistries
                .stream()
                .filter(this::isUsedRegistryAsConfigCenter)
                .map(this::registryAsConfigCenter)
                .forEach(configCenter -> {
                    if (configManager.getConfigCenter(configCenter.getId()).isPresent()) {
                        return;
                    }
                    configManager.addConfigCenter(configCenter);
                    logger.info("use registry as config-center: " + configCenter);

                });
        }
    }

    private boolean isUsedRegistryAsConfigCenter(RegistryConfig registryConfig) {
        return isUsedRegistryAsCenter(registryConfig, registryConfig::getUseAsConfigCenter, "config",
            DynamicConfigurationFactory.class);
    }

    private ConfigCenterConfig registryAsConfigCenter(RegistryConfig registryConfig) {
        String protocol = registryConfig.getProtocol();
        Integer port = registryConfig.getPort();
        URL url = URL.valueOf(registryConfig.getAddress(), registryConfig.getScopeModel());
        String id = "config-center-" + protocol + "-" + url.getHost() + "-" + port;
        ConfigCenterConfig cc = new ConfigCenterConfig();
        cc.setId(id);
        cc.setScopeModel(applicationModel);
        if (cc.getParameters() == null) {
            cc.setParameters(new HashMap<>());
        }
        if (registryConfig.getParameters() != null) {
            cc.getParameters().putAll(registryConfig.getParameters()); // copy the parameters
        }
        cc.getParameters().put(CLIENT_KEY, registryConfig.getClient());
        cc.setProtocol(protocol);
        cc.setPort(port);
        if (StringUtils.isNotEmpty(registryConfig.getGroup())) {
            cc.setGroup(registryConfig.getGroup());
        }
        cc.setAddress(getRegistryCompatibleAddress(registryConfig));
        cc.setNamespace(registryConfig.getGroup());
        cc.setUsername(registryConfig.getUsername());
        cc.setPassword(registryConfig.getPassword());
        if (registryConfig.getTimeout() != null) {
            cc.setTimeout(registryConfig.getTimeout().longValue());
        }
        cc.setHighestPriority(false);
        return cc;
    }

    private void useRegistryAsMetadataCenterIfNecessary() {

        Collection<MetadataReportConfig> metadataConfigs = configManager.getMetadataConfigs();

        if (CollectionUtils.isNotEmpty(metadataConfigs)) {
            return;
        }

        List<RegistryConfig> defaultRegistries = configManager.getDefaultRegistries();
        if (defaultRegistries.size() > 0) {
            defaultRegistries
                .stream()
                .filter(this::isUsedRegistryAsMetadataCenter)
                .map(this::registryAsMetadataCenter)
                .forEach(metadataReportConfig -> {
                    Optional<MetadataReportConfig> configOptional = configManager.getConfig(MetadataReportConfig.class, metadataReportConfig.getId());
                    if (configOptional.isPresent()) {
                        return;
                    }
                    configManager.addMetadataReport(metadataReportConfig);
                    logger.info("use registry as metadata-center: " + metadataReportConfig);
                });
        }
    }

    private boolean isUsedRegistryAsMetadataCenter(RegistryConfig registryConfig) {
        return isUsedRegistryAsCenter(registryConfig, registryConfig::getUseAsMetadataCenter, "metadata",
            MetadataReportFactory.class);
    }

    /**
     * Is used the specified registry as a center infrastructure
     *
     * @param registryConfig       the {@link RegistryConfig}
     * @param usedRegistryAsCenter the configured value on
     * @param centerType           the type name of center
     * @param extensionClass       an extension class of a center infrastructure
     * @return
     * @since 2.7.8
     */
    private boolean isUsedRegistryAsCenter(RegistryConfig registryConfig, Supplier<Boolean> usedRegistryAsCenter,
                                           String centerType,
                                           Class<?> extensionClass) {
        final boolean supported;

        Boolean configuredValue = usedRegistryAsCenter.get();
        if (configuredValue != null) { // If configured, take its value.
            supported = configuredValue.booleanValue();
        } else {                       // Or check the extension existence
            String protocol = registryConfig.getProtocol();
            supported = supportsExtension(extensionClass, protocol);
            if (logger.isInfoEnabled()) {
                logger.info(format("No value is configured in the registry, the %s extension[name : %s] %s as the %s center"
                    , extensionClass.getSimpleName(), protocol, supported ? "supports" : "does not support", centerType));
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info(format("The registry[%s] will be %s as the %s center", registryConfig,
                supported ? "used" : "not used", centerType));
        }
        return supported;
    }

    /**
     * Supports the extension with the specified class and name
     *
     * @param extensionClass the {@link Class} of extension
     * @param name           the name of extension
     * @return if supports, return <code>true</code>, or <code>false</code>
     * @since 2.7.8
     */
    private boolean supportsExtension(Class<?> extensionClass, String name) {
        if (isNotEmpty(name)) {
            ExtensionLoader extensionLoader = getExtensionLoader(extensionClass);
            return extensionLoader.hasExtension(name);
        }
        return false;
    }

    private MetadataReportConfig registryAsMetadataCenter(RegistryConfig registryConfig) {
        String protocol = registryConfig.getProtocol();
        URL url = URL.valueOf(registryConfig.getAddress(), registryConfig.getScopeModel());
        String id = "metadata-center-" + protocol + "-" + url.getHost() + "-" + url.getPort();
        MetadataReportConfig metadataReportConfig = new MetadataReportConfig();
        metadataReportConfig.setId(id);
        metadataReportConfig.setScopeModel(applicationModel);
        if (metadataReportConfig.getParameters() == null) {
            metadataReportConfig.setParameters(new HashMap<>());
        }
        if (registryConfig.getParameters() != null) {
            metadataReportConfig.getParameters().putAll(registryConfig.getParameters()); // copy the parameters
        }
        metadataReportConfig.getParameters().put(CLIENT_KEY, registryConfig.getClient());
        metadataReportConfig.setGroup(registryConfig.getGroup());
        metadataReportConfig.setAddress(getRegistryCompatibleAddress(registryConfig));
        metadataReportConfig.setUsername(registryConfig.getUsername());
        metadataReportConfig.setPassword(registryConfig.getPassword());
        metadataReportConfig.setTimeout(registryConfig.getTimeout());
        return metadataReportConfig;
    }

    private String getRegistryCompatibleAddress(RegistryConfig registryConfig) {
        String registryAddress = registryConfig.getAddress();
        String[] addresses = REGISTRY_SPLIT_PATTERN.split(registryAddress);
        if (ArrayUtils.isEmpty(addresses)) {
            throw new IllegalStateException("Invalid registry address found.");
        }
        String address = addresses[0];
        // since 2.7.8
        // Issue : https://github.com/apache/dubbo/issues/6476
        StringBuilder metadataAddressBuilder = new StringBuilder();
        URL url = URL.valueOf(address, registryConfig.getScopeModel());
        String protocolFromAddress = url.getProtocol();
        if (isEmpty(protocolFromAddress)) {
            // If the protocol from address is missing, is like :
            // "dubbo.registry.address = 127.0.0.1:2181"
            String protocolFromConfig = registryConfig.getProtocol();
            metadataAddressBuilder.append(protocolFromConfig).append("://");
        }
        metadataAddressBuilder.append(address);
        return metadataAddressBuilder.toString();
    }

    /**
     * Initialize {@link MetadataService} from {@link WritableMetadataService}'s extension
     */
    private void initMetadataService() {
//        startMetadataCenter();
        this.metadataService = getExtensionLoader(WritableMetadataService.class).getDefaultExtension();
        // support injection by super type MetadataService
        applicationModel.getBeanFactory().registerBean(this.metadataService);

        //this.metadataServiceExporter = new ConfigurableMetadataServiceExporter(metadataService);
        this.metadataServiceExporter = getExtensionLoader(MetadataServiceExporter.class).getDefaultExtension();
    }

    /**
     * Start the bootstrap
     *
     * @return
     */
    @Override
    public synchronized Future start() {
        CompletableFuture startFuture = getStartFuture();

        // maybe call start again after add new module, check if any new module
        boolean hasPendingModule = hasPendingModule();

        if (isStarting()) {
            // currently is starting, maybe both start by module and application
            // if has new modules, start them
            if (hasPendingModule) {
                startModules();
            }
            // if is starting, reuse previous startFuture
            return startFuture;
        }

        // if is started and no new module, just return
        if (isStarted() && !hasPendingModule) {
            completeStartFuture(false);
            return startFuture;
        }

        onStarting();

        initialize();

        doStart();

        return startFuture;
    }

    private boolean hasPendingModule() {
        boolean found = false;
        for (ModuleModel moduleModel : applicationModel.getModuleModels()) {
            if (moduleModel.getDeployer().isPending()) {
                found = true;
                break;
            }
        }
        return found;
    }

    private CompletableFuture getStartFuture() {
        if (startFuture == null) {
            synchronized (this) {
                if (startFuture == null) {
                    startFuture = new CompletableFuture();
                }
            }
        }
        return startFuture;
    }


    private void doStart() {
        startModules();

        // prepare application instance
        prepareApplicationInstance();

        executorRepository.getSharedExecutor().submit(() -> {
            while (true) {
                // notify on each module started
                synchronized (startedLock) {
                    try {
                        startedLock.wait(500);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                }

                // if has new module, do start again
                if (hasPendingModule()) {
                    startModules();
                    continue;
                }

                DeployState newState = checkState();
                if (!(newState == DeployState.STARTING || newState == DeployState.PENDING)) {
                    // start finished or error
                    break;
                }
            }
        });
    }

    private void startModules() {
        // copy current modules, ignore new module during starting
        List<ModuleModel> moduleModels = new ArrayList<>(applicationModel.getModuleModels());
        for (ModuleModel moduleModel : moduleModels) {
            // export services in module
            if (moduleModel.getDeployer().isPending()) {
                moduleModel.getDeployer().start();
            }
        }
    }

    @Override
    public void prepareApplicationInstance() {
        if (hasPreparedApplicationInstance.get()) {
            return;
        }
        // if register consumer instance or has exported services
        if (isRegisterConsumerInstance() || hasExportedServices()) {
            if (!hasPreparedApplicationInstance.compareAndSet(false, true)) {
                return;
            }
            prepareInternalModule();
            // register the local ServiceInstance if required
            registerServiceInstance();
        }
    }

    private void prepareInternalModule() {
        // export MetadataService
        exportMetadataService();
        // start internal module
        ModuleDeployer internalModuleDeployer = applicationModel.getInternalModule().getDeployer();
        if (!internalModuleDeployer.isStarted()) {
            Future future = internalModuleDeployer.start();
            // wait for internal module start finished
            try {
                future.get();
            } catch (Exception e) {
                logger.warn("wait for internal module started failed: " + e.getMessage(), e);
            }
        }
    }

    private boolean hasExportedServices() {
        for (ModuleModel moduleModel : applicationModel.getModuleModels()) {
            if (CollectionUtils.isNotEmpty(moduleModel.getConfigManager().getServices())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isBackground() {
        for (ModuleModel moduleModel : applicationModel.getModuleModels()) {
            if (moduleModel.getDeployer().isBackground()) {
                return true;
            }
        }
        return false;
    }

    private DynamicConfiguration prepareEnvironment(ConfigCenterConfig configCenter) {
        if (configCenter.isValid()) {
            if (!configCenter.checkOrUpdateInitialized(true)) {
                return null;
            }

            DynamicConfiguration dynamicConfiguration = null;
            try {
                dynamicConfiguration = getDynamicConfiguration(configCenter.toUrl());
            } catch (Exception e) {
                if (!configCenter.isCheck()) {
                    logger.warn("The configuration center failed to initialize", e);
                    configCenter.checkOrUpdateInitialized(false);
                    return null;
                } else {
                    throw new IllegalStateException(e);
                }
            }

            String configContent = dynamicConfiguration.getProperties(configCenter.getConfigFile(), configCenter.getGroup());

            String appGroup = getApplication().getName();
            String appConfigContent = null;
            if (isNotEmpty(appGroup)) {
                appConfigContent = dynamicConfiguration.getProperties
                    (isNotEmpty(configCenter.getAppConfigFile()) ? configCenter.getAppConfigFile() : configCenter.getConfigFile(),
                        appGroup
                    );
            }
            try {
                environment.updateExternalConfigMap(parseProperties(configContent));
                environment.updateAppExternalConfigMap(parseProperties(appConfigContent));
            } catch (IOException e) {
                throw new IllegalStateException("Failed to parse configurations from Config Center.", e);
            }
            return dynamicConfiguration;
        }
        return null;
    }

    /**
     * Get the instance of {@link DynamicConfiguration} by the specified connection {@link URL} of config-center
     *
     * @param connectionURL of config-center
     * @return non-null
     * @since 2.7.5
     */
    private DynamicConfiguration getDynamicConfiguration(URL connectionURL) {
        String protocol = connectionURL.getProtocol();

        DynamicConfigurationFactory factory = ConfigurationUtils.getDynamicConfigurationFactory(applicationModel, protocol);
        return factory.getDynamicConfiguration(connectionURL);
    }

    /**
     * export {@link MetadataService}
     */
    private void exportMetadataService() {
        metadataServiceExporter.export();
    }

    private void unexportMetadataService() {
        if (metadataServiceExporter != null && metadataServiceExporter.isExported()) {
            try {
                metadataServiceExporter.unexport();
            } catch (Exception ignored) {
                // ignored
            }
        }
    }

    private void registerServiceInstance() {
        if (isRegisteredServiceInstance()) {
            return;
        }

        ApplicationConfig application = getApplication();
        String serviceName = application.getName();
        ServiceInstance serviceInstance = createServiceInstance(serviceName);
        boolean registered = true;
        try {
            ServiceInstanceMetadataUtils.registerMetadataAndInstance(serviceInstance);
        } catch (Exception e) {
            registered = false;
            logger.error("Register instance error", e);
        }
        if (registered) {
            // scheduled task for updating Metadata and ServiceInstance
            asyncMetadataFuture = executorRepository.getSharedScheduledExecutor().scheduleAtFixedRate(() -> {

                // ignore refresh metadata on stopping
                if (applicationModel.isDestroyed()) {
                    return;
                }

                InMemoryWritableMetadataService localMetadataService = (InMemoryWritableMetadataService) WritableMetadataService.getDefaultExtension(applicationModel);
                localMetadataService.blockUntilUpdated();
                try {
                    if (!applicationModel.isDestroyed()) {
                        ServiceInstanceMetadataUtils.refreshMetadataAndInstance(serviceInstance);
                    }
                } catch (Exception e) {
                    if (!applicationModel.isDestroyed()) {
                        logger.error("Refresh instance and metadata error", e);
                    }
                } finally {
                    localMetadataService.releaseBlock();
                }
            }, 0, ConfigurationUtils.get(applicationModel, METADATA_PUBLISH_DELAY_KEY, DEFAULT_METADATA_PUBLISH_DELAY), TimeUnit.MILLISECONDS);
        }
    }

    private boolean isRegisteredServiceInstance() {
        return this.serviceInstance != null;
    }

    private void doRegisterServiceInstance(ServiceInstance serviceInstance) {
        // register instance only when at least one service is exported.
        if (serviceInstance.getPort() > 0) {
            publishMetadataToRemote(serviceInstance);
            logger.info("Start registering instance address to registry.");
            RegistryManager.getInstance(applicationModel).getServiceDiscoveries().forEach(serviceDiscovery ->
            {
                ServiceInstance serviceInstanceForRegistry = new DefaultServiceInstance((DefaultServiceInstance) serviceInstance);
                calInstanceRevision(serviceDiscovery, serviceInstanceForRegistry);
                if (logger.isDebugEnabled()) {
                    logger.info("Start registering instance address to registry" + serviceDiscovery.getUrl() + ", instance " + serviceInstanceForRegistry);
                }
                // register metadata
                serviceDiscovery.register(serviceInstanceForRegistry);
            });
        }
    }

    private void publishMetadataToRemote(ServiceInstance serviceInstance) {
//        InMemoryWritableMetadataService localMetadataService = (InMemoryWritableMetadataService)WritableMetadataService.getDefaultExtension();
//        localMetadataService.blockUntilUpdated();
        if (logger.isInfoEnabled()) {
            logger.info("Start publishing metadata to remote center, this only makes sense for applications enabled remote metadata center.");
        }
        RemoteMetadataServiceImpl remoteMetadataService = applicationModel.getBeanFactory().getBean(RemoteMetadataServiceImpl.class);
        remoteMetadataService.publishMetadata(serviceInstance.getServiceName());
    }

    private void unregisterServiceInstance() {
        if (isRegisteredServiceInstance()) {
            RegistryManager.getInstance(applicationModel).getServiceDiscoveries().forEach(serviceDiscovery -> {
                try {
                    serviceDiscovery.unregister(serviceInstance);
                } catch (Exception ignored) {
                    // ignored
                }
            });
        }
    }

    private ServiceInstance createServiceInstance(String serviceName) {
        this.serviceInstance = new DefaultServiceInstance(serviceName, applicationModel);
        setMetadataStorageType(serviceInstance, getMetadataType());
        ServiceInstanceMetadataUtils.customizeInstance(this.serviceInstance);
        return this.serviceInstance;
    }

    public ServiceInstance getServiceInstance() {
        return serviceInstance;
    }

    @Override
    public void stop() {
        applicationModel.destroy();
    }

    @Override
    public void preDestroy() {
        if (isStopping() || isStopped()) {
            return;
        }
        onStopping();

        unRegisterShutdownHook();
        if (asyncMetadataFuture != null) {
            asyncMetadataFuture.cancel(true);
        }
        unregisterServiceInstance();
        unexportMetadataService();

    }

    @Override
    public synchronized void postDestroy() {
        // expect application model is destroyed before here
        if (isStopped()) {
            return;
        }
        try {
            executeShutdownCallbacks();

            destroyRegistries();
            destroyServiceDiscoveries();
            destroyMetadataReports();

            // TODO should we close unused protocol server which only used by this application?
            // protocol server will be closed on all applications of same framework are stopped currently, but no associate to application
            // see org.apache.dubbo.config.deploy.FrameworkModelCleaner#destroyProtocols
            // see org.apache.dubbo.config.bootstrap.DubboBootstrapMultiInstanceTest#testMultiProviderApplicationStopOneByOne

            // destroy all executor services
            destroyExecutorRepository();

            onStopped();
        } catch (Throwable ex) {
            logger.error(getIdentifier() + " an error occurred while stopping application: " + ex.getMessage(), ex);
            setFailed(ex);
        }
    }

    private void executeShutdownCallbacks() {
        ShutdownHookCallbacks shutdownHookCallbacks = applicationModel.getBeanFactory().getBean(ShutdownHookCallbacks.class);
        shutdownHookCallbacks.callback();
    }

    @Override
    public void checkStarting() {
        if (isStarting()) {
            return;
        }
        for (ModuleModel moduleModel : applicationModel.getModuleModels()) {
            if (moduleModel.getDeployer().isStarting()) {
                onStarting();
                break;
            }
        }
    }

    @Override
    public void checkStarted() {
        // TODO improve newState checking
        DeployState newState = checkState();
        switch (newState) {
            case STARTED:
                onStarted();
                break;
            case STARTING:
                onStarting();
                break;
            case PENDING:
                setPending();
                break;
        }

        // notify started
        synchronized (startedLock) {
            startedLock.notifyAll();
        }
    }

    private DeployState checkState() {
        DeployState newState = DeployState.UNKNOWN;
        int pending = 0, starting = 0, started = 0, stopping = 0, stopped = 0;
        for (ModuleModel moduleModel : applicationModel.getModuleModels()) {
            ModuleDeployer deployer = moduleModel.getDeployer();
            if (deployer.isPending()) {
                pending++;
            } else if (deployer.isStarting()) {
                starting++;
            } else if (deployer.isStarted()) {
                started++;
            } else if (deployer.isStopping()) {
                stopping++;
            } else if (deployer.isStopped()) {
                stopped++;
            }
        }

        if (started > 0) {
            if (pending + starting + stopping + stopped == 0) {
                // all modules have been started
                newState = DeployState.STARTED;
            } else if (pending + starting > 0) {
                // some module is pending and some is started
                newState = DeployState.STARTING;
            } else if (stopping + stopped > 0) {
                newState = DeployState.STOPPING;
            }
        } else if (starting > 0) {
            // any module is starting
            newState = DeployState.STARTING;
        } else if (pending > 0) {
            if (starting + starting + stopping + stopped == 0) {
                // all modules have not starting or started
                newState = DeployState.PENDING;
            } else if (stopping + stopped > 0) {
                // some is pending and some is stopping or stopped
                newState = DeployState.STOPPING;
            }
        } else if (stopping > 0) {
            // some is stopping and some stopped
            newState = DeployState.STOPPING;
        } else if (stopped > 0) {
            // all modules are stopped
            newState = DeployState.STOPPED;
        }
        return newState;
    }

    private void onStarting() {
        if (isStarting()) {
            return;
        }
        setStarting();
        if (logger.isInfoEnabled()) {
            logger.info(getIdentifier() + " is starting.");
        }
    }

    private void onStarted() {
        if (isStarted()) {
            return;
        }
        setStarted();
        if (logger.isInfoEnabled()) {
            logger.info(getIdentifier() + " is ready.");
        }
        // refresh metadata
        try {
            if (serviceInstance != null) {
                ServiceInstanceMetadataUtils.refreshMetadataAndInstance(serviceInstance);
            }
        } catch (Exception e) {
            logger.error("refresh metadata failed: " + e.getMessage(), e);
        }
        // complete future
        completeStartFuture(true);
        // shutdown export/refer executor after started
        executorRepository.shutdownServiceExportExecutor();
        executorRepository.shutdownServiceReferExecutor();
    }

    private void completeStartFuture(boolean success) {
        if (startFuture != null) {
            startFuture.complete(success);
            startFuture = null;
        }
    }

    private void onStopping() {
        if (isStopping()) {
            return;
        }
        setStopping();
        if (logger.isInfoEnabled()) {
            logger.info(getIdentifier() + " is stopping.");
        }
    }

    private void onStopped() {
        if (isStopped()) {
            return;
        }
        setStopped();
        if (logger.isInfoEnabled()) {
            logger.info(getIdentifier() + " has stopped.");
        }

    }

    private void destroyExecutorRepository() {
        getExtensionLoader(ExecutorRepository.class).getDefaultExtension().destroyAll();
    }

    private void destroyRegistries() {
        RegistryManager.getInstance(applicationModel).destroyAll();
    }

    private void destroyServiceDiscoveries() {
        RegistryManager.getInstance(applicationModel).getServiceDiscoveries().forEach(serviceDiscovery -> {
            try {
                serviceDiscovery.destroy();
            } catch (Throwable ignored) {
                logger.warn(ignored.getMessage(), ignored);
            }
        });
        if (logger.isDebugEnabled()) {
            logger.debug(getIdentifier() + "'s all ServiceDiscoveries have been destroyed.");
        }
    }

    private void destroyMetadataReports() {
        // only destroy MetadataReport of this application
        List<MetadataReportFactory> metadataReportFactories = getExtensionLoader(MetadataReportFactory.class).getLoadedExtensionInstances();
        for (MetadataReportFactory metadataReportFactory : metadataReportFactories) {
            metadataReportFactory.destroy();
        }
    }

    private ApplicationConfig getApplication() {
        return configManager.getApplicationOrElseThrow();
    }

    private String getIdentifier() {
        if (identifier == null) {
            identifier = "Dubbo application[" + applicationModel.getInternalId() + "]";
            if (applicationModel.getModelName() != null
                && !StringUtils.isEquals(applicationModel.getModelName(), applicationModel.getInternalName())) {
                identifier += "(" + applicationModel.getModelName() + ")";
            }
        }
        return identifier;
    }

}
