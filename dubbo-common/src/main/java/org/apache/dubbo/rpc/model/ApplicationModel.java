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
package org.apache.dubbo.rpc.model;

import org.apache.dubbo.common.config.Environment;
import org.apache.dubbo.common.context.ApplicationExt;
import org.apache.dubbo.common.deploy.ApplicationDeployer;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.extension.ExtensionScope;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.config.ApplicationConfig;
import org.apache.dubbo.config.context.ConfigManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@link ExtensionLoader}, {@code DubboBootstrap} and this class are at present designed to be
 * singleton or static (by itself totally static or uses some static fields). So the instances
 * returned from them are of process scope. If you want to support multiple dubbo servers in one
 * single process, you may need to refactor those three classes.
 * <p>
 * Represent a application which is using Dubbo and store basic metadata info for using
 * during the processing of RPC invoking.
 * <p>
 * ApplicationModel includes many ProviderModel which is about published services
 * and many Consumer Model which is about subscribed services.
 * <p>
 */

public class ApplicationModel extends ScopeModel {
    protected static final Logger LOGGER = LoggerFactory.getLogger(ApplicationModel.class);
    public static final String NAME = "ApplicationModel";

    // static volatile定义了一个自己的类型，static+volatile组合，定义自己的类型
    // 单例模式
    private static volatile ApplicationModel defaultInstance;
    // 包含了多个module models
    private final List<ModuleModel> moduleModels = Collections.synchronizedList(new ArrayList<>());
    private final List<ModuleModel> pubModuleModels = Collections.synchronizedList(new ArrayList<>());
    // 环境变量、配置信息
    private Environment environment;
    // 服务配置相关的一些信息
    private ConfigManager configManager;
    // 服务数据相关的一些存储
    private ServiceRepository serviceRepository;
    // 属于application层级的一些组件的生命周期管理
    private ApplicationDeployer deployer;
    // 父级组件
    private final FrameworkModel frameworkModel;
    // 内部的一个module model组件
    private ModuleModel internalModule;
    // 默认的一个module model组件
    private volatile ModuleModel defaultModule;

    // internal module index is 0, default module index is 1
    private AtomicInteger moduleIndex = new AtomicInteger(0);
    // 是一个锁
    private Object moduleLock = new Object();


    // --------- static methods ----------//

    public static ApplicationModel ofNullable(ApplicationModel applicationModel) {
        if (applicationModel != null) {
            return applicationModel;
        } else {
            return defaultModel();
        }
    }

    // 走了一个double check+volatile+static，就可以实现一个单例设计模式
    // double里大量的都是采用了这种方式来实现的单例模式
    public static ApplicationModel defaultModel() {
        if (defaultInstance == null) {
            synchronized (ApplicationModel.class) {
                if (defaultInstance == null) {
                    // 而且在这里，会给他传入 一个framework model
                    // application model的父类，是framework model，module model->application model-> framework model，组成了一个体系
                    defaultInstance = new ApplicationModel(FrameworkModel.defaultModel());
                }
            }
        }
        return defaultInstance;
    }

    /**
     * @deprecated use {@link ServiceRepository#allConsumerModels()}
     */
    @Deprecated
    public static Collection<ConsumerModel> allConsumerModels() {
        return defaultModel().getApplicationServiceRepository().allConsumerModels();
    }

    /**
     * @deprecated use {@link ServiceRepository#allProviderModels()}
     */
    @Deprecated
    public static Collection<ProviderModel> allProviderModels() {
        return defaultModel().getApplicationServiceRepository().allProviderModels();
    }

    /**
     * @deprecated use {@link FrameworkServiceRepository#lookupExportedService(String)}
     */
    @Deprecated
    public static ProviderModel getProviderModel(String serviceKey) {
        return defaultModel().getDefaultModule().getServiceRepository().lookupExportedService(serviceKey);
    }

    /**
     * @deprecated ConsumerModel should fetch from context
     */
    @Deprecated
    public static ConsumerModel getConsumerModel(String serviceKey) {
        return defaultModel().getDefaultModule().getServiceRepository().lookupReferredService(serviceKey);
    }

    /**
     * @deprecated Replace to {@link ScopeModel#getModelEnvironment()}
     */
    @Deprecated
    public static Environment getEnvironment() {
        return defaultModel().getModelEnvironment();
    }

    /**
     * @deprecated Replace to {@link ApplicationModel#getApplicationConfigManager()}
     */
    @Deprecated
    public static ConfigManager getConfigManager() {
        return defaultModel().getApplicationConfigManager();
    }

    /**
     * @deprecated Replace to {@link ApplicationModel#getApplicationServiceRepository()}
     */
    @Deprecated
    public static ServiceRepository getServiceRepository() {
        return defaultModel().getApplicationServiceRepository();
    }

    /**
     * @deprecated Replace to {@link ApplicationModel#getApplicationExecutorRepository()}
     */
    @Deprecated
    public static ExecutorRepository getExecutorRepository() {
        return defaultModel().getApplicationExecutorRepository();
    }

    /**
     * @deprecated Replace to {@link ApplicationModel#getCurrentConfig()}
     */
    @Deprecated
    public static ApplicationConfig getApplicationConfig() {
        return defaultModel().getCurrentConfig();
    }

    /**
     * @deprecated Replace to {@link ApplicationModel#getApplicationName()}
     */
    @Deprecated
    public static String getName() {
        return defaultModel().getCurrentConfig().getName();
    }

    /**
     * @deprecated Replace to {@link ApplicationModel#getApplicationName()}
     */
    @Deprecated
    public static String getApplication() {
        return getName();
    }

    // only for unit test
    @Deprecated
    public static void reset() {
        if (defaultInstance != null) {
            defaultInstance.destroy();
            defaultInstance = null;
        }
    }

    // ------------- instance methods ---------------//

    public ApplicationModel(FrameworkModel frameworkModel) {
        // 父级组件，就是framework model
        super(frameworkModel, ExtensionScope.APPLICATION);
        Assert.notNull(frameworkModel, "FrameworkModel can not be null");
        this.frameworkModel = frameworkModel;
        frameworkModel.addApplication(this);
        initialize();
        // bind to default instance if absent
        if (defaultInstance == null) {
            defaultInstance = this;
        }
    }

    @Override
    protected void initialize() {
        super.initialize();
        internalModule = new ModuleModel(this, true);
        this.serviceRepository = new ServiceRepository(this);

        // 通过SPI机制，去获取了ApplicationInitListener接口的扩展实现，回调
        ExtensionLoader<ApplicationInitListener> extensionLoader = this.getExtensionLoader(ApplicationInitListener.class);
        Set<String> listenerNames = extensionLoader.getSupportedExtensions();
        for (String listenerName : listenerNames) {
            extensionLoader.getExtension(listenerName).init();
        }

        initApplicationExts();

        ExtensionLoader<ScopeModelInitializer> initializerExtensionLoader = this.getExtensionLoader(ScopeModelInitializer.class);
        Set<ScopeModelInitializer> initializers = initializerExtensionLoader.getSupportedExtensionInstances();
        for (ScopeModelInitializer initializer : initializers) {
            initializer.initializeApplicationModel(this);
        }
    }

    private void initApplicationExts() {
        Set<ApplicationExt> exts = this.getExtensionLoader(ApplicationExt.class).getSupportedExtensionInstances();
        for (ApplicationExt ext : exts) {
            ext.initialize();
        }
    }

    @Override
    protected void onDestroy() {

        if (deployer != null) {
            deployer.preDestroy();
        }

        // destroy application resources
        for (ModuleModel moduleModel : new ArrayList<>(moduleModels)) {
            if (moduleModel != internalModule) {
                moduleModel.destroy();
            }
        }
        // destroy internal module later
        internalModule.destroy();

        if (defaultInstance == this) {
            synchronized (ApplicationModel.class) {
                frameworkModel.removeApplication(this);
                defaultInstance = null;
            }
        } else {
            frameworkModel.removeApplication(this);
        }

        if (deployer != null) {
            deployer.postDestroy();
        }

        // destroy other resources (e.g. ZookeeperTransporter )
        notifyDestroy();

        if (environment != null) {
            environment.destroy();
            environment = null;
        }
        if (configManager != null) {
            configManager.destroy();
            configManager = null;
        }
        if (serviceRepository != null) {
            serviceRepository.destroy();
            serviceRepository = null;
        }
        // try destroy framework if no any application
        frameworkModel.tryDestroy();
    }

    public FrameworkModel getFrameworkModel() {
        return frameworkModel;
    }
    public ModuleModel newModule() {
        return new ModuleModel(this);
    }

    @Override
    public Environment getModelEnvironment() {
        if (environment == null) {
            environment = (Environment) this.getExtensionLoader(ApplicationExt.class)
                .getExtension(Environment.NAME);
        }
        return environment;
    }

    public ConfigManager getApplicationConfigManager() {
        if (configManager == null) {
            // 会通过我们之前讲解的SPI机制
            // SPI的机制，最经典的用法，就是在这里
            configManager = (ConfigManager) this.getExtensionLoader(ApplicationExt.class)
                .getExtension(ConfigManager.NAME);
        }
        return configManager;
    }

    public ServiceRepository getApplicationServiceRepository() {
        return serviceRepository;
    }

    public ExecutorRepository getApplicationExecutorRepository() {
        // 其实每次都是通过SPI机制来获取到对应的线程池repository
        return this.getExtensionLoader(ExecutorRepository.class).getDefaultExtension();
    }

    public ApplicationConfig getCurrentConfig() {
        return getApplicationConfigManager().getApplicationOrElseThrow();
    }

    public String getApplicationName() {
        return getCurrentConfig().getName();
    }

    public String tryGetApplicationName() {
        Optional<ApplicationConfig> appCfgOptional = getApplicationConfigManager().getApplication();
        return appCfgOptional.isPresent() ? appCfgOptional.get().getName() : null;
    }

    void addModule(ModuleModel moduleModel, boolean isInternal) {
        synchronized (moduleLock) {
            if (!this.moduleModels.contains(moduleModel)) {
                this.moduleModels.add(moduleModel);
                moduleModel.setInternalName(buildInternalName(ModuleModel.NAME, getInternalId(), moduleIndex.getAndIncrement()));
                if (!isInternal) {
                    pubModuleModels.add(moduleModel);
                }
            }
        }
    }

    public void removeModule(ModuleModel moduleModel) {
        synchronized (moduleLock) {
            this.moduleModels.remove(moduleModel);
            this.pubModuleModels.remove(moduleModel);
            if (moduleModel == defaultModule) {
                defaultModule = findDefaultModule();
            }
        }
    }

    void tryDestroy() {
        if (this.moduleModels.isEmpty()
            || (this.moduleModels.size() == 1 && this.moduleModels.get(0) == internalModule)) {
            destroy();
        }
    }

    public List<ModuleModel> getModuleModels() {
        return Collections.unmodifiableList(moduleModels);
    }

    public List<ModuleModel> getPubModuleModels() {
        return Collections.unmodifiableList(pubModuleModels);
    }

    public ModuleModel getDefaultModule() {
        if (defaultModule == null) {
            if (isDestroyed()) {
                return null;
            }
            synchronized (moduleLock) {
                if (defaultModule == null) {
                    defaultModule = findDefaultModule();
                    if (defaultModule == null) {
                        defaultModule = this.newModule();
                    }
                }
            }
        }
        return defaultModule;
    }

    private ModuleModel findDefaultModule() {
        for (ModuleModel moduleModel : moduleModels) {
            if (moduleModel != internalModule) {
                return moduleModel;
            }
        }
        return null;
    }

    public ModuleModel getInternalModule() {
        return internalModule;
    }

    /**
     * @deprecated only for ut
     */
    @Deprecated
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    /**
     * @deprecated only for ut
     */
    @Deprecated
    public void setConfigManager(ConfigManager configManager) {
        this.configManager = configManager;
    }

    /**
     * @deprecated only for ut
     */
    @Deprecated
    public void setServiceRepository(ServiceRepository serviceRepository) {
        this.serviceRepository = serviceRepository;
    }

    @Override
    public void addClassLoader(ClassLoader classLoader) {
        super.addClassLoader(classLoader);
        if (environment != null) {
            environment.refreshClassLoaders();
        }
    }

    @Override
    public void removeClassLoader(ClassLoader classLoader) {
        super.removeClassLoader(classLoader);
        if (environment != null) {
            environment.refreshClassLoaders();
        }
    }

    @Override
    protected boolean checkIfClassLoaderCanRemoved(ClassLoader classLoader) {
        return super.checkIfClassLoaderCanRemoved(classLoader) && !containsClassLoader(classLoader);
    }

    protected boolean containsClassLoader(ClassLoader classLoader) {
        return moduleModels.stream().anyMatch(moduleModel -> moduleModel.getClassLoaders().contains(classLoader));
    }

    public ApplicationDeployer getDeployer() {
        return deployer;
    }

    public void setDeployer(ApplicationDeployer deployer) {
        this.deployer = deployer;
    }
}
