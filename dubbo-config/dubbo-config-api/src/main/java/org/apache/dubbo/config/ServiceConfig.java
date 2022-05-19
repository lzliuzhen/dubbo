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
package org.apache.dubbo.config;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.deploy.ModuleDeployer;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.url.component.ServiceConfigURL;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.annotation.Service;
import org.apache.dubbo.config.invoker.DelegateProviderMetaDataInvoker;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.config.utils.ConfigValidationUtils;
import org.apache.dubbo.metadata.MetadataService;
import org.apache.dubbo.metadata.ServiceNameMapping;
import org.apache.dubbo.metadata.WritableMetadataService;
import org.apache.dubbo.registry.client.metadata.MetadataUtils;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProxyFactory;
import org.apache.dubbo.rpc.cluster.ConfiguratorFactory;
import org.apache.dubbo.rpc.model.ModuleServiceRepository;
import org.apache.dubbo.rpc.model.ProviderModel;
import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.model.ServiceDescriptor;
import org.apache.dubbo.rpc.service.GenericService;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_IP_TO_BIND;
import static org.apache.dubbo.common.constants.CommonConstants.LOCALHOST_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.METADATA_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.METHODS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.MONITOR_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PROVIDER_SIDE;
import static org.apache.dubbo.common.constants.CommonConstants.REGISTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REMOTE_METADATA_STORAGE_TYPE;
import static org.apache.dubbo.common.constants.CommonConstants.REVISION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SERVICE_NAME_MAPPING_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.SIDE_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.SERVICE_REGISTRY_PROTOCOL;
import static org.apache.dubbo.common.utils.NetUtils.getAvailablePort;
import static org.apache.dubbo.common.utils.NetUtils.getLocalHost;
import static org.apache.dubbo.common.utils.NetUtils.isInvalidLocalHost;
import static org.apache.dubbo.common.utils.NetUtils.isInvalidPort;
import static org.apache.dubbo.config.Constants.DUBBO_IP_TO_REGISTRY;
import static org.apache.dubbo.config.Constants.DUBBO_PORT_TO_BIND;
import static org.apache.dubbo.config.Constants.DUBBO_PORT_TO_REGISTRY;
import static org.apache.dubbo.config.Constants.SCOPE_NONE;
import static org.apache.dubbo.remoting.Constants.BIND_IP_KEY;
import static org.apache.dubbo.remoting.Constants.BIND_PORT_KEY;
import static org.apache.dubbo.rpc.Constants.GENERIC_KEY;
import static org.apache.dubbo.rpc.Constants.LOCAL_PROTOCOL;
import static org.apache.dubbo.rpc.Constants.PROXY_KEY;
import static org.apache.dubbo.rpc.Constants.SCOPE_KEY;
import static org.apache.dubbo.rpc.Constants.SCOPE_LOCAL;
import static org.apache.dubbo.rpc.Constants.SCOPE_REMOTE;
import static org.apache.dubbo.rpc.Constants.TOKEN_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.EXPORT_KEY;

public class ServiceConfig<T> extends ServiceConfigBase<T> {

    private static final long serialVersionUID = 7868244018230856253L;

    private static final Logger logger = LoggerFactory.getLogger(ServiceConfig.class);

    /**
     * A random port cache, the different protocols who has no port specified have different random port
     */
    private static final Map<String, Integer> RANDOM_PORT_MAP = new HashMap<String, Integer>();

    private Protocol protocolSPI;

    /**
     * A {@link ProxyFactory} implementation that will generate a exported service proxy,the JavassistProxyFactory is its
     * default implementation
     */
    private ProxyFactory proxyFactory;

    private ProviderModel providerModel;

    /**
     * Whether the provider has been exported
     */
    private transient volatile boolean exported;

    /**
     * The flag whether a service has unexported ,if the method unexported is invoked, the value is true
     */
    private transient volatile boolean unexported;

    private transient volatile AtomicBoolean initialized = new AtomicBoolean(false);

    /**
     * The exported services
     */
    private final List<Exporter<?>> exporters = new ArrayList<Exporter<?>>();

    private final List<ServiceListener> serviceListeners = new ArrayList<>();
    private WritableMetadataService localMetadataService;

    public ServiceConfig() {
    }

    public ServiceConfig(Service service) {
        super(service);
    }

    @Override
    protected void postProcessAfterScopeModelChanged(ScopeModel oldScopeModel, ScopeModel newScopeModel) {
        super.postProcessAfterScopeModelChanged(oldScopeModel, newScopeModel);

        // 结合这个使用实例，去看一下对应的自适应获取extension实例的机制
        // SPI机制，动态的获取对应的实现类以及实例对象

        // 我们在看完了大量的dubbo源码之后，应该就是对dubbo源码有了一个比较深刻的认识
        // 在dubbo源码里我们看到了大量的SPI机制的使用
        // 很多地方对一些核心组件的接口实现类的获取/选择，实例对象的构建，都是用SPI机制来做的
        // SPI机制，只要指定一个接口，他就会自动按照一个规则，在项目目录下面，指定目录里面，去查找这个接口对应的SPI配置文件
        // 对这个SPI配置文件里面的内容做一个读取，就代表了这个接口对应的一些可以供选择的实现类
        // 到底使用哪个或者哪些实现类，此时可以基于SPI机制的按name获取，adaptive自适应获取，activate自动激活批量获取
        // 拿到对应的实现类之后，就直接对实现类构建他的一个实例，基于jdk的反射技术就可以做到了，clazz.newInstance()就可以做到了

        // SPI机制，核心，扩展，我们可以任意的把dubbo里的核心组件，实现，替换成我们自己的实现
        // 按照SPI机制配置的规则，在指定目录下配置指定接口的实现类，就可以了，实现类可以是我们自己的实现类
        // 在dubbo框架运行的过程中，对核心组件都会基于接口进行SPI查找，必然会找到我们自己的实现，替换
        // 整个dubbo框架，各种核心组件，其实都是跟实现，是松耦合的

        // 如果说要是dubbo要是没设计这套SPI机制，也想实现一套核心组件可扩展和替换的机制
        // 他可以设计一些配置项，他自己可以有一些配置文件，在配置文件里可以有类似于protocol.class=xx，你可以去做一个配置
        // 他在源码里，对具体的Protocol接口的实现类的获取，可以基于配置文件去做一个读取，如果没配置，就有一个默认的类
        // 也能实现你的框架的核心组件的高度可扩展，留出对应的配置项，你想替换和扩展哪个核心组件，就去进行配置

        // 那么dubbo SPI机制，到底设计他、使用他、优点到底在哪里？
        // 仅仅使用配置文件和配置项来做，此时是很死板的，你的protocol.class只能配置一个类，如果你希望的是可以有多个实现类
        // 在运行过程中根据上下文环境里不同的参数或者属性，来动态的选择具体的实现类
        // 如果你一次性要能够拿到多个实现类，但是要有选择性的拿到多个实现类，protocol.class=xx,xx,xx，三个类，但是你一次性就希望获取到里面的2个实现类

        // 综上所述，dubbo如果想要实现一套特别好的动态扩展机制，必然要引入SPI机制
        // 按name获取实现类、adaptive动态自适应机制、activate批量激活机制，可以帮助dubbo应对各种各样的扩展场景

        // 这里拿到的是一个adaptive自适应的Protocol接口，代理
        protocolSPI = this.getExtensionLoader(Protocol.class).getAdaptiveExtension();
        proxyFactory = this.getExtensionLoader(ProxyFactory.class).getAdaptiveExtension();

        localMetadataService = this.getScopeModel().getDefaultExtension(WritableMetadataService.class);
    }

    @Override
    @Parameter(excluded = true, attribute = false)
    public boolean isExported() {
        return exported;
    }


    @Override
    @Parameter(excluded = true, attribute = false)
    public boolean isUnexported() {
        return unexported;
    }

    @Override
    public void unexport() {
        if (!exported) {
            return;
        }
        if (unexported) {
            return;
        }
        if (!exporters.isEmpty()) {
            for (Exporter<?> exporter : exporters) {
                try {
                    exporter.unexport();
                } catch (Throwable t) {
                    logger.warn("Unexpected error occured when unexport " + exporter, t);
                }
            }
            exporters.clear();
        }
        unexported = true;
        onUnexpoted();
        ModuleServiceRepository repository = getScopeModel().getServiceRepository();
        repository.unregisterProvider(providerModel);
    }

    /**
     * for early init serviceMetadata
     */
    public void init() {
        // 在一个中间件里面，会大量的运用到juc的技术，java并发
        if (this.initialized.compareAndSet(false, true)) {
            // load ServiceListeners from extension
            ExtensionLoader<ServiceListener> extensionLoader = this.getExtensionLoader(ServiceListener.class);
            this.serviceListeners.addAll(extensionLoader.getSupportedExtensionInstances());
        }
        // 初始化你的service metadata，服务元数据
        // metadata center，元数据中心，配合起来来看他
        // service metadata，服务实例的元数据，也就是说对服务实例做一个描述的元数据
        // 最最核心的，就是service他的对应接口，实现类到底是哪个类
        initServiceMetadata(provider);
        serviceMetadata.setServiceType(getInterfaceClass());
        serviceMetadata.setTarget(getRef());
        serviceMetadata.generateServiceKey();
    }

    @Override
    public synchronized void export() {
        if (this.exported) {
            return;
        }

        // 一开始是没有办法给大家讲解这些why
        // 如果连what都没看懂，dubbo大量源码细节，你都没看过，此时讲why是非常不靠谱的
        // model而言，在dubbo里大量的地方都会去使用他，不是说直接使用model
        // getXxModel().getXx() -> 为什么说dubbo里从来不会直接去使用model呢，都是从model提取一些东西来使用
        // model的本质，透过现象去看到本质，类似于设计模式里门面模式的经典思想，facade
        // model当做一个对外的门面，门里面封装了很多的其他的一些组件，这些组件的话呢，都是在各种地方都会要去使用
        // 公共的组件
        // 当我们把大量的公共组件，SPI（extension）、Repository、其他的组件，在dubbo框架运行过程中经常会使用到这些组件
        // 把这些组件都封装在对应的model里面，我们在任何地方要使用公共组件，通过model组件就可以去获取了

        // why，逆向思考
        // 如果dubbo没有设计一个model组件体系和机制，此时会出现什么样的一个问题呢？
        // 也不能说是大问题，可能会导致代码的混乱
        // 在各种地方，如果你要使用SPI、Repository、其他的组件，可能各种不同的地方都会使用自己的一些方法去获取和使用这些组件
        // dubbo，框架，开源技术，公共组件的获取和使用，往往是一个难点
        // 多人协作开发，A、B、C、D几十个人，几百个人，一起给一个开源框架贡献代码
        // 他们其实贡献的是不同地方的代码，势必会使用到一些公共的组件，他们不同的人是完全不同的一个搞法，统一标准
        // SPI，A可能是ExtensionDirector.get().get；B，可能会自己去new ExetensionXxx组件，再去.get()
        // 代码看起来极为的混乱
        // 对大量的公共组件的封装、暴露和使用，其实通过一个标准化的model组件，都给你封装好，方法都写好
        // 大量的人，在不同的地方写代码，都知道一个点，你就先去获取到一个model，通过他的方法调用，标准化，获取到公共组件，用就可以了
        // 对大量的类似的代码有一个集中封装，统一使用标准和方法的效果

        // model：module、application、framework
        // 利用model体系 -> 多层级结构 -> module里有自己范围内的一批组件
        // application大范围内有一批共享的组件，framework级别 又有一批组件

        // prepare for export
        // dubbo 2.6.x和2.7.x源码我都看过，dubbo 3.0源码变动还是有点大的
        // ModuleDeployer组件
        // dubbo服务实例内部必须有很多的代码组件，如果零零散散的去调用、初始化
        // 初步判定：对服务实例的启动，做很多的初始化准备工作
        ModuleDeployer moduleDeployer = getScopeModel().getDeployer();
        // 使用这个组件，就可以去进行服务实例的准备工作
        // 在这里，会对我们的metadata report组件做一个构建和初始化，以及启动（建立跟zk的连接）
        moduleDeployer.prepare();

        if (!this.isRefreshed()) {
            // 执行服务实例的刷新操作，搞好了ProviderConfig->MethodConfig->ArgumentConfig体系
            this.refresh();
        }
        if (this.shouldExport()) {
            // 自己就会执行服务实例的初始化的工作，metadata
            // 在这里就会把我们的metadata元数据给他准备好，后续就可以准备去进行元数据上报了
            this.init();

            // dubbo服务实例的延迟发布的特性
            // 如果设置了dubbo服务实例是延迟发布的，当你调用了export方法之后，会进到这里
            // 他会延迟你指定的时间之后，再去进行服务的一个发布
            if (shouldDelay()) {
                doDelayExport();
            } else {
                // 核心的服务对外发布的源码流程，就在这里
                // 到源码里需要去寻找答案
                // 看源码技巧，源码一旦运行过后，控制台就会打印很多的东西出来
                // 我们就可以先去分析里面的log日志，通过日志，去分析和判断到底有什么东西，会做什么
                // 反过来再提出问题，带着问题，到核心的源码入口里去，一步一步的寻找答案

                // 1、export dubbo service，动作，发布dubbo服务实例，如何发布，什么叫做发布
                // 2、register dubbo service，动作，肯定是往zk里进行注册
                // 3、启动netty server，网络连接监听和请求处理，网络通信这块基于netty要能够启动
                // 4、服务发现注册相关工作
                // 5、MetadataReport：服务实例上报
                // 6、关闭jvm的时候，会有一个逆向操作的处理过程

                doExport(); // 就是他作为一个服务实例里面的服务发布这个功能场景的一个入口
            }

            // notify export this service
            // 会执行一个通知操作，通知你的export service
            moduleDeployer.notifyExportService(this);
        }
    }

    /**
     * export service only, do not register application instance, for exporting services in batches by module
     */
    @Override
    public synchronized void exportOnly() {
        if (this.exported) {
            return;
        }
        if (!this.isRefreshed()) {
            this.refresh();
        }
        if (this.shouldExport()) {
            this.init();

            if (shouldDelay()) {
                doDelayExport();
            } else {
                doExport();
            }
        }
    }

    protected void doDelayExport() {
        getScopeModel().getDefaultExtension(ExecutorRepository.class).getServiceExportExecutor()
            .schedule(() -> {
                try {
                    doExport();
                } catch (Exception e) {
                    logger.error("Failed to export service config: " + interfaceName, e);
                }
            }, getDelay(), TimeUnit.MILLISECONDS);
    }

    protected void exported() {
        exported = true;
        List<URL> exportedURLs = this.getExportedUrls();
        exportedURLs.forEach(url -> {
            if (url.getParameters().containsKey(SERVICE_NAME_MAPPING_KEY)) {
                ServiceNameMapping serviceNameMapping = ServiceNameMapping.getDefaultExtension(getScopeModel());
                try {
                    boolean succeeded = serviceNameMapping.map(url);
                    if (succeeded) {
                        logger.info("Successfully registered interface application mapping for service " + url.getServiceKey());
                    }
                } catch (Exception e) {
                    logger.error("Failed register interface application mapping for service " + url.getServiceKey(), e);
                }
            }
        });
        onExported();
    }

    private void checkAndUpdateSubConfigs() {

        // Use default configs defined explicitly with global scope
        completeCompoundConfigs();

        checkProtocol();

        // init some null configuration.
        // 他这里有一个SPI机制的使用，是去使用了activate自动激活的机制
        // 在整个阅读dubbo源码细节的过程之中，我们看到了SPI的使用，adaptive自适应都可以去仔细看一下SPI的细节
        List<ConfigInitializer> configInitializers = this.getExtensionLoader(ConfigInitializer.class)
                .getActivateExtension(URL.valueOf("configInitializer://", getScopeModel()), (String[]) null);
        configInitializers.forEach(e -> e.initServiceConfig(this));

        // if protocol is not injvm checkRegistry
        if (!isOnlyInJvm()) {
            checkRegistry();
        }

        if (StringUtils.isEmpty(interfaceName)) {
            throw new IllegalStateException("<dubbo:service interface=\"\" /> interface not allow null!");
        }

        // ref就是我们自己的实现类
        // 下面的逻辑里，都会根据你的接口名称，去获取到对应的一些接口class
        if (ref instanceof GenericService) {
            interfaceClass = GenericService.class;
            if (StringUtils.isEmpty(generic)) {
                generic = Boolean.TRUE.toString();
            }
        } else {
            try {
                if (getInterfaceClassLoader() != null) {
                    interfaceClass = Class.forName(interfaceName, true, getInterfaceClassLoader());
                } else {
                    interfaceClass = Class.forName(interfaceName, true, Thread.currentThread().getContextClassLoader());
                }
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            checkRef();
            generic = Boolean.FALSE.toString();
        }

        if (local != null) {
            if ("true".equals(local)) {
                local = interfaceName + "Local";
            }
            Class<?> localClass;
            try {
                localClass = ClassUtils.forNameWithThreadContextClassLoader(local);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            if (!interfaceClass.isAssignableFrom(localClass)) {
                throw new IllegalStateException("The local implementation class " + localClass.getName() + " not implement interface " + interfaceName);
            }
        }
        if (stub != null) {
            if ("true".equals(stub)) {
                stub = interfaceName + "Stub";
            }
            Class<?> stubClass;
            try {
                stubClass = ClassUtils.forNameWithThreadContextClassLoader(stub);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            if (!interfaceClass.isAssignableFrom(stubClass)) {
                throw new IllegalStateException("The stub implementation class " + stubClass.getName() + " not implement interface " + interfaceName);
            }
        }
        checkStubAndLocal(interfaceClass);
        ConfigValidationUtils.checkMock(interfaceClass, this);
        ConfigValidationUtils.validateServiceConfig(this);
        postProcessConfig();
    }

    @Override
    protected void postProcessRefresh() {
        super.postProcessRefresh();
        checkAndUpdateSubConfigs();
    }

    protected synchronized void doExport() {
        if (unexported) {
            throw new IllegalStateException("The service " + interfaceClass.getName() + " has already unexported!");
        }
        if (exported) {
            return;
        }

        if (StringUtils.isEmpty(path)) {
            path = interfaceName;
        }
        doExportUrls(); // 发布服务
        exported(); // 服务已经发布完成了
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void doExportUrls() {
        // 在这里分析一下这块源码，ScopeModel，真实的类型叫做ModuleModel
        // getScopeModel()，再去获取service repository，以及之前也获取过其他的组件
        // dubbo这里，把他的各个组件，都集中在了ScopeModel=ModuleModel，ScopeModel就类似于设计模式里的门面模式
        // ScopeModel、ModuleModel、ApplicationModel、FrameworkModel，多个Model组成一个体系，里面会包含一些组件


        // Repository存储组件设计的思想
        // module自己本身是没什么用的，facade，背后封装了一系列的组件，方便我们在代码运作过程中以
        // 标准化、便捷化的方式去获取各种组件来使用

        // 分成几种不同类别的Repository，ServiceRepository，顾名思义，放的都是一些服务相关的数据
        // 在dubbo框架里，对各种不同门类的数据，服务数据，配置数据，线程池，公共数据存取点，运行过程中，随时可能要存取各种公共数据
        // 此时必然就需要有一些存取公共数据的组件，存储+读取/使用，存储是核心含义，设计了一系列的Repository组件思想
        // 不同的Repository组件就去放不同的数据，那么再运行过程中，就可以随时随地对你需要的公共数据进行一个存储和读取，只要获取那个数据对应的Repository组件了

        // 如果说你要是没有这种存储组件的话，很可能大量的人在协同开发的时候，会出一个问题
        // 可能不同的人把同一种数据在进行存储和读取的时候，可能会通过各种各样混乱的不同的方式来进行
        // 会把你的dubbo代码，写的极为的混乱

        // 服务实例数据
        // 设计一个ServiceRepository，可能会把一些服务实例数据拿出来以后，就不去用这个ServiceRepository
        // ServiceManager组件，自己把自己需要的数据，都放在manager组件里面


        // ServiceRepository又是什么东西
        // ServiceRepository，核心本质是dubbo服务数据存储组件
        // 一个系统其实是可以发布多个dubbo服务，每个dubbo服务的本质和核心就是一个interface和一个实现类
        ModuleServiceRepository repository = getScopeModel().getServiceRepository();
        // 把当前要发布的服务注册到了dubbo服务数据存储组件里去了，repository
        ServiceDescriptor serviceDescriptor = repository.registerService(getInterfaceClass());

        // 服务提供者，你是暴露服务出去的，provider，通过所有的信息，封装了一个ProviderModel
        providerModel = new ProviderModel(getUniqueServiceName(),
            ref,
            serviceDescriptor,
            this,
            getScopeModel(),
            serviceMetadata);
        // 又可以基于Repository组件把provider数据注册进去
        repository.registerProvider(providerModel);

        // 生成的注册URL，本身是2181的端口号，是针对zk进行注册的
        List<URL> registryURLs = ConfigValidationUtils.loadRegistries(this, true);

        for (ProtocolConfig protocolConfig : protocols) {
            String pathKey = URL.buildKey(getContextPath(protocolConfig)
                    .map(p -> p + "/" + path)
                    .orElse(path), group, version);
            // In case user specified path, register service one more time to map it to path.
            repository.registerService(pathKey, interfaceClass);
            doExportUrlsFor1Protocol(protocolConfig, registryURLs);
        }
    }

    private void doExportUrlsFor1Protocol(ProtocolConfig protocolConfig, List<URL> registryURLs) {
        Map<String, String> map = buildAttributes(protocolConfig);

        //init serviceMetadata attachments
        serviceMetadata.getAttachments().putAll(map);

        URL url = buildUrl(protocolConfig, registryURLs, map);

        exportUrl(url, registryURLs);
    }

    private Map<String, String> buildAttributes(ProtocolConfig protocolConfig) {

        Map<String, String> map = new HashMap<String, String>();
        map.put(SIDE_KEY, PROVIDER_SIDE);

        // append params with basic configs,
        ServiceConfig.appendRuntimeParameters(map);
        AbstractConfig.appendParameters(map, getApplication());
        AbstractConfig.appendParameters(map, getModule());
        // remove 'default.' prefix for configs from ProviderConfig
        // appendParameters(map, provider, Constants.DEFAULT_KEY);
        AbstractConfig.appendParameters(map, provider);
        AbstractConfig.appendParameters(map, protocolConfig);
        AbstractConfig.appendParameters(map, this);
        appendMetricsCompatible(map);

        MetadataReportConfig metadataReportConfig = getMetadataReportConfig();
        if (metadataReportConfig != null && metadataReportConfig.isValid()) {
            map.putIfAbsent(METADATA_KEY, REMOTE_METADATA_STORAGE_TYPE);
        }

        // append params with method configs,
        if (CollectionUtils.isNotEmpty(getMethods())) {
            getMethods().forEach(method -> appendParametersWithMethod(method, map));
        }

        if (ProtocolUtils.isGeneric(generic)) {
            map.put(GENERIC_KEY, generic);
            map.put(METHODS_KEY, ANY_VALUE);
        } else {
            String revision = Version.getVersion(interfaceClass, version);
            if (revision != null && revision.length() > 0) {
                map.put(REVISION_KEY, revision);
            }

            String[] methods = methods(interfaceClass);
            if (methods.length == 0) {
                logger.warn("No method found in service interface " + interfaceClass.getName());
                map.put(METHODS_KEY, ANY_VALUE);
            } else {
                map.put(METHODS_KEY, StringUtils.join(new HashSet<String>(Arrays.asList(methods)), ","));
            }
        }

        /**
         * Here the token value configured by the provider is used to assign the value to ServiceConfig#token
         */
        if (ConfigUtils.isEmpty(token) && provider != null) {
            token = provider.getToken();
        }

        if (!ConfigUtils.isEmpty(token)) {
            if (ConfigUtils.isDefault(token)) {
                map.put(TOKEN_KEY, UUID.randomUUID().toString());
            } else {
                map.put(TOKEN_KEY, token);
            }
        }

        return map;
    }

    private void appendParametersWithMethod(MethodConfig method, Map<String, String> params) {
        AbstractConfig.appendParameters(params, method, method.getName());

        String retryKey = method.getName() + ".retry";
        if (params.containsKey(retryKey)) {
            String retryValue = params.remove(retryKey);
            if ("false".equals(retryValue)) {
                params.put(method.getName() + ".retries", "0");
            }
        }

        List<ArgumentConfig> arguments = method.getArguments();
        if (CollectionUtils.isNotEmpty(arguments)) {
            Method matchedMethod = findMatchedMethod(method);
            if (matchedMethod != null) {
                arguments.forEach(argument -> appendArgumentConfig(argument, matchedMethod, params));
            }
        }
    }

    private Method findMatchedMethod(MethodConfig methodConfig) {
        for (Method method : interfaceClass.getMethods()) {
            if (method.getName().equals(methodConfig.getName())) {
                return method;
            }
        }
        return null;
    }

    private void appendArgumentConfig(ArgumentConfig argument, Method method, Map<String, String> params) {
        if (StringUtils.isNotEmpty(argument.getType())) {
            Integer index = findArgumentIndexIndexWithGivenType(argument, method);
            AbstractConfig.appendParameters(params, argument, method.getName() + "." + index);
        } else if (hasIndex(argument)) {
            AbstractConfig.appendParameters(params, argument, method.getName() + "." + argument.getIndex());
        } else {
            throw new IllegalArgumentException("Argument config must set index or type attribute.eg: <dubbo:argument index='0' .../> or <dubbo:argument type=xxx .../>");
        }
    }

    private boolean hasIndex(ArgumentConfig argument) {
        return argument.getIndex() != -1;
    }

    private boolean isTypeMatched(String type, Integer index, Class<?>[] argtypes) {
        return index != null && index >= 0 && index < argtypes.length && argtypes[index].getName().equals(type);
    }

    private Integer findArgumentIndexIndexWithGivenType(ArgumentConfig argument, Method method) {
        Class<?>[] argtypes = method.getParameterTypes();
        // one callback in the method
        if (hasIndex(argument)) {
            Integer index = argument.getIndex();
            String type = argument.getType();
            if (isTypeMatched(type, index, argtypes)) {
                return index;
            } else {
                throw new IllegalArgumentException("Argument config error : the index attribute and type attribute not match :index :" + argument.getIndex() + ", type:" + argument.getType());
            }
        } else {
            // multiple callbacks in the method
            for (int j = 0; j < argtypes.length; j++) {
                if (isTypeMatched(argument.getType(), j, argtypes)) {
                    return j;
                }
            }
            throw new IllegalArgumentException("Argument config error : no argument matched with the type:" + argument.getType());
        }
    }

    private URL buildUrl(ProtocolConfig protocolConfig, List<URL> registryURLs, Map<String, String> params) {
        String name = protocolConfig.getName();
        if (StringUtils.isEmpty(name)) {
            name = DUBBO;
        }

        // export service
        String host = findConfigedHosts(protocolConfig, registryURLs, params);
        Integer port = findConfigedPorts(protocolConfig, name, params);
        URL url = new ServiceConfigURL(name, null, null, host, port, getContextPath(protocolConfig).map(p -> p + "/" + path).orElse(path), params);
        url.setScopeModel(getScopeModel());

        // You can customize Configurator to append extra parameters
        if (this.getExtensionLoader(ConfiguratorFactory.class)
                .hasExtension(url.getProtocol())) {
            url = this.getExtensionLoader(ConfiguratorFactory.class)
                    .getExtension(url.getProtocol()).getConfigurator(url).configure(url);
        }
        url = url.setScopeModel(getScopeModel());
        url =  url.setServiceModel(providerModel);
        return url;
    }

    private void exportUrl(URL url, List<URL> registryURLs) {
        String scope = url.getParameter(SCOPE_KEY);
        // don't export when none is configured
        if (!SCOPE_NONE.equalsIgnoreCase(scope)) {

            // export to local if the config is not remote (export to remote only when config is remote)
            if (!SCOPE_REMOTE.equalsIgnoreCase(scope)) {
                // 必须先进行本地的发布
                exportLocal(url);
            }

            // export to remote if the config is not local (export to local only when config is local)
            if (!SCOPE_LOCAL.equalsIgnoreCase(scope)) {
                // 他会先去进行远程发布，在zk里进行注册，还有做一个网络发布，这两个事情做完了，provider服务实例就完成了启动了
                // 就需要把这个服务实例，元数据，推送到元数据中心里去
                // 动态配置中心，如果你有需要，可以从里面去获取参数，如果监听配置项的变更，可以去加监听器
                url = exportRemote(url, registryURLs);
                // MetadataUtils，做一个publish发布服务定义
                MetadataUtils.publishServiceDefinition(url);
            }

        }
        this.urls.add(url);
    }

    // 需要确保说，你的服务可以对外提供访问
    private URL exportRemote(URL url, List<URL> registryURLs) {
        if (CollectionUtils.isNotEmpty(registryURLs)) {

            // 会发现在dubbo里有一个很关键的一个东西，URL，这个URL里我们发现有很多的信息在里面
            // 协议、各种各样的参数、各种各样的信息，url就可以给后续的代码的运行提供很多需要的东西在里面
            // 为什么要有URL这样的一个东西呢？承载一些配置和信息
            // 在代码运转的过程中，consumer和Provider进行交互的过程中
            for (URL registryURL : registryURLs) {
                if (SERVICE_REGISTRY_PROTOCOL.equals(registryURL.getProtocol())) {
                    url = url.addParameterIfAbsent(SERVICE_NAME_MAPPING_KEY, "true");
                }

                //if protocol is only injvm ,not register
                if (LOCAL_PROTOCOL.equalsIgnoreCase(url.getProtocol())) {
                    continue;
                }

                url = url.addParameterIfAbsent(DYNAMIC_KEY, registryURL.getParameter(DYNAMIC_KEY));
                URL monitorUrl = ConfigValidationUtils.loadMonitor(this, registryURL);
                if (monitorUrl != null) {
                    url = url.putAttribute(MONITOR_KEY, monitorUrl);
                }

                // For providers, this is used to enable custom proxy to generate invoker
                String proxy = url.getParameter(PROXY_KEY);
                if (StringUtils.isNotEmpty(proxy)) {
                    registryURL = registryURL.addParameter(PROXY_KEY, proxy);
                }

                if (logger.isInfoEnabled()) {
                    if (url.getParameter(REGISTER_KEY, true)) {
                        logger.info("Register dubbo service " + interfaceClass.getName() + " url " + url.getServiceKey() + " to registry " + registryURL.getAddress());
                    } else {
                        logger.info("Export dubbo service " + interfaceClass.getName() + " to url " + url.getServiceKey());
                    }
                }

                doExportUrl(registryURL.putAttribute(EXPORT_KEY, url), true);
            }

        } else {

            if (MetadataService.class.getName().equals(url.getServiceInterface())) {
                localMetadataService.setMetadataServiceURL(url);
            }

            if (logger.isInfoEnabled()) {
                logger.info("Export dubbo service " + interfaceClass.getName() + " to url " + url);
            }

            doExportUrl(url, true);
        }


        return url;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void doExportUrl(URL url, boolean withMetaData) {

        // ProxyFactory，Proxy，动态代理
        // ref，实现类
        // interfaceClass，接口
        // url服务实例对外暴露出去的一些核心信息
        // Invoker调用组件，当dubbo的netty server对外网络监听到连接，处理请求，必须要对请求有一个调用组件，可以去调
        // ProxyFactory基于我们的DemoService接口生成的动态代理，被调用接口的时候，底层会回调你自己写的实现类，DemoServiceImpl

        // 动态代理技术有很多种，cglib，jdk，动态代理技术如果不了解的话，可以自己去看一下
        // 面向一个接口，动态生成接口的一个实现类 ，对这个实现类动态生成对应的对象，动态代理的对象
        // 对象必然会代理自己背后的一个实现类
        // 当这个对象被调用的时候，背后的实现类其实就是会被调用

        // 默认情况下，封装一个proxyinvoker，就是代理invoker，后续针对本地实现类代理了他，对实现类进行调用
        // 用javassist技术生成wrapper，proxyinvoker调用，底层就是基于javassist wrapper在进行调用
        // proxy invoker，代理invoker，负责转发对目标实现类的调用
        // jdk，都是封装一个invoker，invoker代理了对目标实现类的一个方法调用，javassist用他的技术去做调用，jdk就是反射技术去做调用

        // proxy代理，我们要把一个服务发布出去，是用来给别人来进行调用的
        // 接口和实现类，接口定义好了我发布出去的服务，到底是别人有哪些方法可以来调用，接口是做了一个严格的定义
        // 必须去调用接口的实现类，实现类里就写了各种方法的实现逻辑，可能是针对mysql的crud

        // 如果我们要是直接把我们的接口和方法暴露出去，给网络通信的组件来进行调用，他们中间会缺少了一个层次
        // 代理层
        // 网络通信组件这块，他在收到一个rpc请求之后，必然会去完成这个rpc请求，针对哪个接口、调用哪个方法、传入进去的各个参数的值
        // 难不成让我们的网络通信组件，直接耦合我们的自定义的接口和实现类吗？
        // 难不成说让我们你在网络通信组件里，去通过类似于javassist技术，或者说是jdk反射技术，去调用我们的一个实现类的方法呢？
        // 通用的网络通信组件代码，直接就跟我们自己写的业务代码，耦合在一起，会导致很严重的代码污染的

        // 我们肯定是要把网络通信这一层要跟我们自己实现的业务代码要做解耦和隔离
        // 隔离网络通信层和我们的实现类这一层，最重要的是什么呢，就是我们的代理层，proxy层
        // 代理，代理，让这一层代理我们的自己实现的实现类
        // 我们的网络通信组件，可以去调用我们的比较通用的代理层的组件就可以了，dubbo框架里面
        // 让代理层，再去代理调用，反过来去调用我们的实现类的方法就可以了

        // 必然要针对实现类去创建和获取一层代理
        // AbstractProxyInvoker，创建一个抽象类的匿名内部子类的实例对象，在里面封装对我们的目标实现类的调用
        // 通过这一个proxy代理层，就可以完美的把我们的业务代码和通用的框架代码，都是隔离开来的


        // dubbo里面，invoker组件设计特别的精准和漂亮
        // 跟dubbo学习一下源码设计的技巧和思想，invoker，大的概念，调用组件，要对服务进行调用的时候
        // 一般来说就得通过invoker来进行
        // invoker又分为了很多种，AbstractProxyInvoker，代理invoker，代理了一个目标对象，针对目标对象，可以进行代理的访问
        // 这个proxy invoker代理了对目标对象的访问
        // cluster invoker -> mock cluster invoker、failover cluster invoker
        // dubbo invoker
        // invoke，调用，invoker，调用者，调用组件，invoker就是代表了对一个什么什么东西的调用的语义在里面
        // 只不过说在执行这个调用的过程中，到了你这个invoker的时候，可能会干一些跟你的语音匹配的一些功能在里面
        // proxy invoker：javassist技术、jdk反射对目标类方法的调用
        // mock cluster invoker：mock降级；failover cluster invoker：故障转移invoker
        // dubbo invoker：基于netty去发起rpc请求

        // rpc请求里，rpc调用，remote procedure call，远程过程调用，call
        // invoker组件，rpc调用的过程中，会有很多的调用步骤和过程，拆分成很多的环节，针对不同的环节，都可以有对应的invoke
        // 不同的invoker实现类负责的就是rpc调用过程中某个步骤和环节

        // 深刻体会和理解invoker组件设计思想
        // rpc调用过程会包含很多的步骤和环节，mock降级、集群容错、网络调用、代理调用
        // 针对rpc调用设计出一个invoker组件，不同的实现类就代表了不同的步骤，多个步骤串联起来运行，完成一次完整的rpc调用
        // proxy invoker，代表了一个针对目标实现类的代理，通过这个代理去调用，也是rpc调用过程里的一个环节，ProxyInvoker

        Invoker<?> invoker = proxyFactory.getInvoker(ref, (Class) interfaceClass, url);
        if (withMetaData) {
            invoker = new DelegateProviderMetaDataInvoker(invoker, this);
        }

        // 进行服务发布他的主要源码，其实都是在Protocol里面
        // 直接调试打进去源码不太好看，但是这种时候往往我们自己要动脑子来进行分析
        // 第一次进行本地发布的时候，看一下Exporter是什么类型的；第二次远程发布也可以看一下

        // 初步的看了一下DubboProtocol，但是当时单单就是看DubboProtocol，感觉看的很迷茫，一堆组件，乱七八糟的运行
        // RegistryProtocol，里面还封装了一个DubboProtocol，RegistryProtocol先执行，先去做服务注册的事情，接着再执行DubboProtocol，启动NettyServer作为网络服务器


        // Protocol组件，设计思想是什么呢？
        // 为什么要有protocol组件，如果没有他，那么此时代码写到这里会如何呢？
        // 服务发布而言，把你的服务注册到注册中心里去，zk；把你的服务进行网络发布，让别人通过网络可以发送请求来调用你
        // protocol这个东西的话，此时大不了你就是自己写两个类组件，包括调用两个类组件去完成两个动作
        // Registry接口 -> ZooKeeperRegistry/NacosRegistry/EurekaRegistry，就直接去完成服务注册就可以了
        // 你可以直接基于网络服务器的模型去构建网络服务器就可以了，把你的proxy invoker的调用，封装在netty handler里面就可以了

        // 可能会存在这样的一个问题
        // 大型、复杂、多人协作开发过程中的协议和标准的定义问题，注册、发布，这些动作，在dubbo框架运行过程中
        // 可能会在很多不同的地方都可能会有，本地发布（流程）、远程发布（流程）
        // 在各种地方，可能都是不同的人在写这块代码，此时可能代码会你写你的，我写我的，标准化的发布以及调用等动作，可能大家都会用各种不同的方式来实现
        // 导致代码的混乱

        // Protocol是一个组件，接口，在里面核心的方法和动作就两个：export、refer
        // export就是发布一个服务，refer就是引用一个服务用于调用
        // 把他定义在一个Protocol协议层组件里，核心的协议动作就是这两个，我们在各种地方和场景里，如果要执行两个标准化的动作
        // 直接就应该是基于protocol协议组件，标准的动作去执行

        // protocolSPI，接口，实现一看就是必然会通过SPI机制加载出来
        // RegistryProtocol、DubboProtocol
        // 设计两个Protocol实现类组件，protocol核心语义就是两个，服务发布，服务引用
        // 如果说我们直接就设计一个DefaultProtocol，服务发布的时候，直接在里面就把服务注册和网络发布都干了
        // 服务引用，就直接把服务发现和订阅，网络连接初始化，把这些事儿给他都干了

        // 耦合和解耦，注册中心的交互、网络发布和连接，完全不大相关的，独立的两个模块
        // 耦合在一起，DefaultProtocol里面，代码耦合太严重了，未来如果要做一些代码扩展，比如说针对网络连接和发布，或者是注册中心做一些代码维护和替换
        // 可能就直接会影响和污染其他的代码
        // 就我个人而言，我还是比较支持，把服务发布和引用两个语义 里，把注册中心和网络连接，解耦
        // RegistryProtocol，顾名思义，包含的主要是注册中心相关的交互逻辑，export，在注册中心里实行注册，refer，从注册中心里拉取服务地址列表以及订阅
        // DubboProtocol，基于dubbo协议，去完成一个网络服务器发布和网络连接，关注的网络这一块
        // 设计思想，就是把注册中心和网络连接，这两块逻辑做了一个强解耦，拆到不同的protocol组件里面去

        // 探讨：dubbo源码，我是极为极为的赞赏的
        // RegistryProtocol和DubboProtocol，设计，我是持有异议，赞成他的解耦的思想
        // 不是太赞成RegistryProtocol作为一个入口，让他再去调用DubboProtocol，调用逻辑，代码调用逻辑关系也比较粗糙一些
        // 如果说假设这块代码让我来设计和实现的话，protocol，default、dubbo、registry
        // 默认先拿到一个DefaultProtocol.export，基于SPI机制的getDefaultExtension
        // SPI机制的自动激活，activate，直接拿到自动激活的dubbo和registry两个protocol，应该可以有一个依次调用的过程和逻辑
        // List<Protocol> protocols = getModel().getExtensionLoader(Protocol.class).getActivateExtesion();
        // for(Protocol protocol : protocols) {
        //   protocol.export(invoker);
        // }

        Exporter<?> exporter = protocolSPI.export(invoker);
        // 这块我们通过看Protocol的接口，就可以理解Protocol对invoker再干什么
        // 把invoker搞出去，搞成一个exporter
        // 后续有请求过来，通过protocol可以拿到一个invoker
        exporters.add(exporter);
    }


    /**
     * always export injvm
     */
    private void exportLocal(URL url) {
        URL local = URLBuilder.from(url)
                .setProtocol(LOCAL_PROTOCOL)
                .setHost(LOCALHOST_VALUE)
                .setPort(0)
                .build();
        local = local.setScopeModel(getScopeModel())
            .setServiceModel(providerModel);
        doExportUrl(local, false);
        // exportLocal，是什么意思？
        // 发布到本地，也就是系统自己内部，叫做本地，jvm内部做一次export发布就可以了
        // 在jvm内部完成了组件之间的一些交互关系和发布
        logger.info("Export dubbo service " + interfaceClass.getName() + " to local registry url : " + local);
    }

    /**
     * Determine if it is injvm
     *
     * @return
     */
    private boolean isOnlyInJvm() {
        return getProtocols().size() == 1
                && LOCAL_PROTOCOL.equalsIgnoreCase(getProtocols().get(0).getName());
    }


    /**
     * Register & bind IP address for service provider, can be configured separately.
     * Configuration priority: environment variables -> java system properties -> host property in config file ->
     * /etc/hosts -> default network address -> first available network address
     *
     * @param protocolConfig
     * @param registryURLs
     * @param map
     * @return
     */
    private String findConfigedHosts(ProtocolConfig protocolConfig,
                                     List<URL> registryURLs,
                                     Map<String, String> map) {
        boolean anyhost = false;

        String hostToBind = getValueFromConfig(protocolConfig, DUBBO_IP_TO_BIND);
        if (hostToBind != null && hostToBind.length() > 0 && isInvalidLocalHost(hostToBind)) {
            throw new IllegalArgumentException("Specified invalid bind ip from property:" + DUBBO_IP_TO_BIND + ", value:" + hostToBind);
        }

        // if bind ip is not found in environment, keep looking up
        if (StringUtils.isEmpty(hostToBind)) {
            hostToBind = protocolConfig.getHost();
            if (provider != null && StringUtils.isEmpty(hostToBind)) {
                hostToBind = provider.getHost();
            }
            if (isInvalidLocalHost(hostToBind)) {
                anyhost = true;
                if (logger.isDebugEnabled()) {
                    logger.info("No valid ip found from environment, try to get local host.");
                }
                hostToBind = getLocalHost();
            }
        }

        map.put(BIND_IP_KEY, hostToBind);

        // registry ip is not used for bind ip by default
        String hostToRegistry = getValueFromConfig(protocolConfig, DUBBO_IP_TO_REGISTRY);
        if (hostToRegistry != null && hostToRegistry.length() > 0 && isInvalidLocalHost(hostToRegistry)) {
            throw new IllegalArgumentException("Specified invalid registry ip from property:" + DUBBO_IP_TO_REGISTRY + ", value:" + hostToRegistry);
        } else if (StringUtils.isEmpty(hostToRegistry)) {
            // bind ip is used as registry ip by default
            hostToRegistry = hostToBind;
        }

        map.put(ANYHOST_KEY, String.valueOf(anyhost));

        return hostToRegistry;
    }


    /**
     * Register port and bind port for the provider, can be configured separately
     * Configuration priority: environment variable -> java system properties -> port property in protocol config file
     * -> protocol default port
     *
     * @param protocolConfig
     * @param name
     * @return
     */
    private Integer findConfigedPorts(ProtocolConfig protocolConfig,
                                      String name,
                                      Map<String, String> map) {
        Integer portToBind = null;

        // parse bind port from environment
        String port = getValueFromConfig(protocolConfig, DUBBO_PORT_TO_BIND);
        portToBind = parsePort(port);

        // if there's no bind port found from environment, keep looking up.
        if (portToBind == null) {
            portToBind = protocolConfig.getPort();
            if (provider != null && (portToBind == null || portToBind == 0)) {
                portToBind = provider.getPort();
            }
            final int defaultPort = this.getExtensionLoader(Protocol.class).getExtension(name).getDefaultPort();
            if (portToBind == null || portToBind == 0) {
                portToBind = defaultPort;
            }
            if (portToBind <= 0) {
                portToBind = getRandomPort(name);
                if (portToBind == null || portToBind < 0) {
                    portToBind = getAvailablePort(defaultPort);
                    putRandomPort(name, portToBind);
                }
            }
        }

        // registry port, not used as bind port by default
        String portToRegistryStr = getValueFromConfig(protocolConfig, DUBBO_PORT_TO_REGISTRY);
        Integer portToRegistry = parsePort(portToRegistryStr);
        if (portToRegistry != null) {
            portToBind = portToRegistry;
        }

        // save bind port, used as url's key later
        map.put(BIND_PORT_KEY, String.valueOf(portToBind));

        return portToBind;
    }

    private Integer parsePort(String configPort) {
        Integer port = null;
        if (configPort != null && configPort.length() > 0) {
            try {
                int intPort = Integer.parseInt(configPort);
                if (isInvalidPort(intPort)) {
                    throw new IllegalArgumentException("Specified invalid port from env value:" + configPort);
                }
                port = intPort;
            } catch (Exception e) {
                throw new IllegalArgumentException("Specified invalid port from env value:" + configPort);
            }
        }
        return port;
    }

    private String getValueFromConfig(ProtocolConfig protocolConfig, String key) {
        String protocolPrefix = protocolConfig.getName().toUpperCase() + "_";
        String value = ConfigUtils.getSystemProperty(protocolPrefix + key);
        if (StringUtils.isEmpty(value)) {
            value = ConfigUtils.getSystemProperty(key);
        }
        return value;
    }

    private Integer getRandomPort(String protocol) {
        protocol = protocol.toLowerCase();
        return RANDOM_PORT_MAP.getOrDefault(protocol, Integer.MIN_VALUE);
    }

    private void putRandomPort(String protocol, Integer port) {
        protocol = protocol.toLowerCase();
        if (!RANDOM_PORT_MAP.containsKey(protocol)) {
            RANDOM_PORT_MAP.put(protocol, port);
            logger.warn("Use random available port(" + port + ") for protocol " + protocol);
        }
    }

    private void postProcessConfig() {
        // SPI的自动激活机制的用处
        // 很多时候，SPI扩展，我们不是说SPI的一个接口就一个实现类可以用
        // SPI扩展接口，有很多的实现类都可以一起来使用
        // @Activate自动激活机制，通过这个自动激活机制，把很多实现类都进行激活
        List<ConfigPostProcessor> configPostProcessors = this.getExtensionLoader(ConfigPostProcessor.class)
                .getActivateExtension(URL.valueOf("configPostProcessor://", getScopeModel()), (String[]) null);
        // 有多个实现类要用，对多个实现类，进行遍历，每个实现类都可以进行处理
        configPostProcessors.forEach(component -> component.postProcessServiceConfig(this));
    }

    public void addServiceListener(ServiceListener listener) {
        this.serviceListeners.add(listener);
    }

    protected void onExported() {
        for (ServiceListener serviceListener : this.serviceListeners) {
            serviceListener.exported(this);
        }
    }

    protected void onUnexpoted() {
        for (ServiceListener serviceListener : this.serviceListeners) {
            serviceListener.unexported(this);
        }
    }
}
