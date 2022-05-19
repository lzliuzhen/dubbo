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

import org.apache.dubbo.common.config.ModuleEnvironment;
import org.apache.dubbo.common.context.ModuleExt;
import org.apache.dubbo.common.deploy.ModuleDeployer;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.extension.ExtensionScope;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.Assert;
import org.apache.dubbo.config.context.ModuleConfigManager;

import java.util.Set;

/**
 * Model of a service module
 *
 * 属于一个service module的model，是属于一个服务模块的model组件模型
 *
 */
public class ModuleModel extends ScopeModel {
    private static final Logger logger = LoggerFactory.getLogger(ModuleModel.class);

    public static final String NAME = "ModuleModel";

    // ApplicationModel，推测是否也是属于一个子门面组件，内部是否也封装了其他的很多组件
    // 在这里是一个引用关系
    private final ApplicationModel applicationModel;
    // service module环境相关的数据，封装的都是各种各样的配置信息
    private ModuleEnvironment moduleEnvironment;
    // 属于一个关键性的组件，服务仓储，repository，仓储，仓储是用来干什么的，存储数据的
    // 存储的都是一些服务的数据
    private ModuleServiceRepository serviceRepository;
    // 存放的也都是一些服务相关的配置数据
    private ModuleConfigManager moduleConfigManager;
    // module deployer组件，管理其他的一些组件和模块的生命周期
    private ModuleDeployer deployer;

    public ModuleModel(ApplicationModel applicationModel) {
        this(applicationModel, false);
    }

    // 在构造module model的时候，会传递进来一个application model
    // module model一般来说，是把application model当做自己的parent父组件的
    public ModuleModel(ApplicationModel applicationModel, boolean isInternal) {
        super(applicationModel, ExtensionScope.MODULE);
        Assert.notNull(applicationModel, "ApplicationModel can not be null");
        this.applicationModel = applicationModel;
        // 很有可能一个app module里可以加入多个module model
        applicationModel.addModule(this, isInternal);
        initialize();
        Assert.notNull(serviceRepository, "ModuleServiceRepository can not be null");
        Assert.notNull(moduleConfigManager, "ModuleConfigManager can not be null");
        Assert.assertTrue(moduleConfigManager.isInitialized(), "ModuleConfigManager can not be initialized");
    }

    @Override
    protected void initialize() {
        super.initialize();
        this.serviceRepository = new ModuleServiceRepository(this);
        this.moduleConfigManager = new ModuleConfigManager(this);
        this.moduleConfigManager.initialize();

        initModuleExt();

        // 通过SPI机制，先获取到ScopeModelInitializer接口的extension loader
        // dubbo为什么说基于SPI机制，把扩展性做的特别好，他几乎把他所有的核心组件都做成可以基于SPI机制进行扩展和自定义
        ExtensionLoader<ScopeModelInitializer> initializerExtensionLoader = this.getExtensionLoader(ScopeModelInitializer.class);
        // 再通过SPI机制，去获取接口对应的extension实现类的实例
        Set<ScopeModelInitializer> initializers = initializerExtensionLoader.getSupportedExtensionInstances();
        // 做一个遍历，直接回调initizlizer的方法这样子，如果说你自己要扩展这里，自定义ScopeModelInitizlizer接口的扩展实现，做一个SPI配置
        // 在dubbo初始化的过程中，立刻就会去回调你自己的SPI扩展
        for (ScopeModelInitializer initializer : initializers) {
            initializer.initializeModuleModel(this);
        }
    }

    private void initModuleExt() {
        Set<ModuleExt> exts = this.getExtensionLoader(ModuleExt.class).getSupportedExtensionInstances();
        for (ModuleExt ext : exts) {
            ext.initialize();
        }
    }

    @Override
    protected void onDestroy() {
        if (deployer != null) {
            deployer.preDestroy();
        }

        applicationModel.removeModule(this);

        if (deployer != null) {
            deployer.postDestroy();
        }

        // destroy other resources
        notifyDestroy();

        if (serviceRepository != null) {
            serviceRepository.destroy();
            serviceRepository = null;
        }

        if (moduleEnvironment != null) {
            moduleEnvironment.destroy();
            moduleEnvironment = null;
        }
        applicationModel.tryDestroy();
    }

    public ApplicationModel getApplicationModel() {
        return applicationModel;
    }

    public ModuleServiceRepository getServiceRepository() {
        return serviceRepository;
    }

    @Override
    public void addClassLoader(ClassLoader classLoader) {
        super.addClassLoader(classLoader);
        if (moduleEnvironment != null) {
            moduleEnvironment.refreshClassLoaders();
        }
    }

    @Override
    public ModuleEnvironment getModelEnvironment() {
        if (moduleEnvironment == null) {
            moduleEnvironment = (ModuleEnvironment) this.getExtensionLoader(ModuleExt.class)
                .getExtension(ModuleEnvironment.NAME);
        }
        return moduleEnvironment;
    }

    public ModuleConfigManager getConfigManager() {
        return moduleConfigManager;
    }

    public ModuleDeployer getDeployer() {
        return deployer;
    }

    public void setDeployer(ModuleDeployer deployer) {
        this.deployer = deployer;
    }

    /**
     * for ut only
     */
    @Deprecated
    public void setModuleEnvironment(ModuleEnvironment moduleEnvironment) {
        this.moduleEnvironment = moduleEnvironment;
    }
}
