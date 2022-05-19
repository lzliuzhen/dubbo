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
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.bytecode.Wrapper;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.config.Environment;
import org.apache.dubbo.common.config.InmemoryConfiguration;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.config.context.ConfigManager;
import org.apache.dubbo.config.support.Parameter;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.model.ScopeModelUtil;
import org.apache.dubbo.rpc.model.ServiceMetadata;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO_VERSION_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INVOKER_LISTENER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.NATIVE;
import static org.apache.dubbo.common.constants.CommonConstants.PID_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REFERENCE_FILTER_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.RELEASE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TAG_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.MetricsConstants.PROTOCOL_PROMETHEUS;


/**
 * AbstractDefaultConfig
 *
 * @export
 */
public abstract class AbstractInterfaceConfig extends AbstractMethodConfig {

    private static final long serialVersionUID = -1559314110797223229L;

    /**
     * The interface name of the exported service
     * 接口名称
     */
    protected String interfaceName;

    /**
     * The classLoader of interface belong to
     * 接口类加载器
     */
    protected ClassLoader interfaceClassLoader;

    /**
     * The remote service version the customer/provider side will reference
     * 版本号
     */
    protected String version;

    /**
     * The remote service group the customer/provider side will reference
     * 服务分组
     */
    protected String group;
    /**
     * 服务元数据
     */
    protected ServiceMetadata serviceMetadata;
    /**
     * Local impl class name for the service interface
     * 本地的实现类的类名
     */
    protected String local;

    /**
     * Local stub class name for the service interface
     * 动态生成的接口的代理
     */
    protected String stub;

    /**
     * Service monitor
     */
    protected MonitorConfig monitor;

    /**
     * Strategies for generating dynamic agents，there are two strategies can be chosen: jdk and javassist
     */
    protected String proxy;

    /**
     * Cluster type
     */
    protected String cluster;

    /**
     * The {@code Filter} when the provider side exposed a service or the customer side references a remote service used,
     * if there are more than one, you can use commas to separate them
     */
    protected String filter;

    /**
     * The Listener when the provider side exposes a service or the customer side references a remote service used
     * if there are more than one, you can use commas to separate them
     */
    protected String listener;

    /**
     * The owner of the service providers
     */
    protected String owner;

    /**
     * Connection limits, 0 means shared connection, otherwise it defines the connections delegated to the current service
     */
    protected Integer connections;

    /**
     * The layer of service providers
     */
    protected String layer;

    /**
     * The application info
     */
    protected ApplicationConfig application;

    /**
     * The module info
     */
    protected ModuleConfig module;

    /**
     * The registry list the service will register to
     * Also see {@link #registryIds}, only one of them will work.
     */
    protected List<RegistryConfig> registries;

    /**
     * The method configuration
     */
    private List<MethodConfig> methods;

    /**
     * The id list of registries the service will register to
     * Also see {@link #registries}, only one of them will work.
     */
    protected String registryIds;

    // connection events
    protected String onconnect;

    /**
     * Disconnection events
     */
    protected String ondisconnect;

    /**
     * The metrics configuration
     */
    protected MetadataReportConfig metadataReportConfig;

    protected ConfigCenterConfig configCenter;

    // callback limits
    private Integer callbacks;
    // the scope for referring/exporting a service, if it's local, it means searching in current JVM only.
    private String scope;

    protected String tag;

    private  Boolean auth;


    /**
     * The url of the reference service
     */
    protected final List<URL> urls = new ArrayList<URL>();

    public List<URL> getExportedUrls() {
        return urls;
    }

    public URL toUrl() {
        return urls.isEmpty() ? null : urls.iterator().next();
    }

    public List<URL> toUrls() {
        return urls;
    }

    @Override
    protected void postProcessAfterScopeModelChanged(ScopeModel oldScopeModel, ScopeModel newScopeModel) {
        super.postProcessAfterScopeModelChanged(oldScopeModel, newScopeModel);
        // remove this config from old ConfigManager
//        if (oldScopeModel != null && oldScopeModel instanceof ModuleModel) {
//            ((ModuleModel)oldScopeModel).getConfigManager().removeConfig(this);
//        }

        // change referenced config's scope model
        ApplicationModel applicationModel = ScopeModelUtil.getApplicationModel(scopeModel);
        if (this.configCenter != null && this.configCenter.getScopeModel() != applicationModel) {
            this.configCenter.setScopeModel(applicationModel);
        }
        if (this.metadataReportConfig != null && this.metadataReportConfig.getScopeModel() != applicationModel) {
            this.metadataReportConfig.setScopeModel(applicationModel);
        }
        if (this.monitor != null && this.monitor.getScopeModel() != applicationModel) {
            this.monitor.setScopeModel(applicationModel);
        }
        if (this.metadataReportConfig != null && this.metadataReportConfig.getScopeModel() != applicationModel) {
            this.metadataReportConfig.setScopeModel(applicationModel);
        }
        if (CollectionUtils.isNotEmpty(this.registries)) {
            this.registries.forEach(registryConfig -> {
                if (registryConfig.getScopeModel() != applicationModel) {
                    registryConfig.setScopeModel(applicationModel);
                }
            });
        }
    }

    /**
     * Check whether the registry config is exists, and then conversion it to {@link RegistryConfig}
     */
    protected void checkRegistry() {
        convertRegistryIdsToRegistries();

        for (RegistryConfig registryConfig : registries) {
            if (!registryConfig.isValid()) {
                throw new IllegalStateException("No registry config found or it's not a valid config! " +
                        "The registry config is: " + registryConfig);
            }
        }
    }

    public static void appendRuntimeParameters(Map<String, String> map) {
        map.put(DUBBO_VERSION_KEY, Version.getProtocolVersion());
        map.put(RELEASE_KEY, Version.getVersion());
        map.put(TIMESTAMP_KEY, String.valueOf(System.currentTimeMillis()));
        if (ConfigUtils.getPid() > 0) {
            map.put(PID_KEY, String.valueOf(ConfigUtils.getPid()));
        }
    }

    /**
     * @deprecated After metrics config is refactored.
     * This method should no longer use and will be deleted in the future.
     */
    @Deprecated
    protected void appendMetricsCompatible(Map<String, String> map) {
        MetricsConfig metricsConfig = getConfigManager().getMetrics().orElse(null);
        if (metricsConfig != null) {
            if (!metricsConfig.getProtocol().equals(PROTOCOL_PROMETHEUS)) {
                map.put("metrics.protocol", metricsConfig.getProtocol());
                map.put("metrics.port", metricsConfig.getPort());
            }
        }
    }

    /**
     * To obtain the method list in the port, use reflection when in native mode and javaassist otherwise.
     * @param interfaceClass
     * @return
     */
    protected String[] methods(Class<?> interfaceClass) {
        boolean isNative = getEnvironment().getConfiguration().getBoolean(NATIVE, false);
        if (isNative) {
            return Arrays.stream(interfaceClass.getMethods()).map(Method::getName).toArray(String[]::new);
        } else {
            return Wrapper.getWrapper(interfaceClass).getMethodNames();
        }
    }

    protected Environment getEnvironment() {
        return getScopeModel().getModelEnvironment();
    }

    /**
     * 最最核心的，就是通过反射技术，对我们暴露的接口、方法和参数进行反射
     * 把方法和参数都进行MethodConfig、ArgumentConfig的一个封装，包括做一些校验处理
     * @param preferredPrefix
     * @param subPropsConfiguration
     */
    @Override
    protected void processExtraRefresh(String preferredPrefix, InmemoryConfiguration subPropsConfiguration) {
        if (StringUtils.hasText(interfaceName)) {
            // 先通过反射技术，拿到了我们的对外暴露服务的接口
            Class<?> interfaceClass = null;
            try {
                interfaceClass = ClassUtils.forName(interfaceName);
            } catch (ClassNotFoundException e) {
                // There may be no interface class when generic call
                return;
            }
            if (!interfaceClass.isInterface()) {
                throw new IllegalStateException(interfaceName+" is not an interface");
            }

            // Auto create MethodConfig/ArgumentConfig according to config props
            Map<String, String> configProperties = subPropsConfiguration.getProperties();
            // 获取到我们对外暴露的接口的各种方法
            Method[] methods = interfaceClass.getMethods();

            // 现在其实是在对你的暴露服务接口进行处理，通过java反射的技术拿到你的接口Class
            // 以及你的接口Class里面的方法，每个方法其实就是一个当前的服务对外暴露的一个可以调用的小接口
            for (Method method : methods) {
                if (ConfigurationUtils.hasSubProperties(configProperties, method.getName())) {
                    MethodConfig methodConfig = getMethodByName(method.getName());
                    // 在这个过程中，非常关键的一点，就是说要把我们的接口里的每个方法都搞一个MethodConfig
                    // 每个MethodConfig里，也都需要要一批ArgumentConfig
                    // why？为什么要做这个事情呢？
                    // 作为你的对外暴露的接口，后续要被人调用，肯定说是会需要访问以及知道你的方法和参数的一些情况
                    // 总不可能每次都是进行反射调用，拿到method和args去进行处理
                    // 还不如刚开始启动，就对你的接口进解析，拿到所有的method和args进行处理

                    // Add method config if not found
                    if (methodConfig == null) {
                        // 会给对外暴露的接口里，每个方法，都会去创建一个对应的MethodConfig
                        methodConfig = new MethodConfig();
                        methodConfig.setName(method.getName());
                        this.addMethod(methodConfig);
                    }

                    // Add argument config
                    // dubbo.service.{interfaceName}.{methodName}.{arg-index}.xxx=xxx
                    java.lang.reflect.Parameter[] arguments = method.getParameters();
                    for (int i = 0; i < arguments.length; i++) {
                        if (getArgumentByIndex(methodConfig, i) == null &&
                            hasArgumentConfigProps(configProperties, methodConfig.getName(), i)) {
                            // 就会对方法里的每个args参数都搞一个对应的ArgumentConfig
                            ArgumentConfig argumentConfig = new ArgumentConfig();
                            argumentConfig.setIndex(i);
                            methodConfig.addArgument(argumentConfig);
                        }
                    }
                }
            }

            // refresh MethodConfigs，刚才解析出来的一些MethodConfig
            // 这个步骤，属于上一个步骤的后置的处理，本质上来说，还是在延续对MethodConfig的一个处理
            List<MethodConfig> methodConfigs = this.getMethods();
            if (methodConfigs != null && methodConfigs.size() > 0) {
                // whether ignore invalid method config
                Object ignoreInvalidMethodConfigVal = getEnvironment().getConfiguration()
                    .getProperty(ConfigKeys.DUBBO_CONFIG_IGNORE_INVALID_METHOD_CONFIG, "false");
                boolean ignoreInvalidMethodConfig = Boolean.parseBoolean(ignoreInvalidMethodConfigVal.toString());

                Class<?> finalInterfaceClass = interfaceClass;
                List<MethodConfig> validMethodConfigs = methodConfigs.stream().filter(methodConfig -> {
                    methodConfig.setParentPrefix(preferredPrefix);
                    methodConfig.setScopeModel(getScopeModel()); // 在这里都要去关联一下model组件
                    methodConfig.refresh();
                    // verify method config
                    return verifyMethodConfig(methodConfig, finalInterfaceClass, ignoreInvalidMethodConfig);
                }).collect(Collectors.toList());

                this.setMethods(validMethodConfigs);
            }
        }

    }

    private boolean verifyMethodConfig(MethodConfig methodConfig, Class<?> interfaceClass, boolean ignoreInvalidMethodConfig) {
        String methodName = methodConfig.getName();
        if (StringUtils.isEmpty(methodName)) {
            String msg = "<dubbo:method> name attribute is required! Please check: " +
                "<dubbo:service interface=\"" + interfaceName + "\" ... >" +
                "<dubbo:method name=\"\" ... /></<dubbo:reference>";
            if (ignoreInvalidMethodConfig) {
                logger.warn(msg);
                return false;
            } else {
                throw new IllegalStateException(msg);
            }
        }

        boolean hasMethod = Arrays.stream(interfaceClass.getMethods()).anyMatch(method -> method.getName().equals(methodName));
        if (!hasMethod) {
            String msg = "Found invalid method config, the interface " + interfaceClass.getName() + " not found method \""
                + methodName + "\" : [" + methodConfig + "]";
            if (ignoreInvalidMethodConfig) {
                logger.warn(msg);
                return false;
            } else {
                throw new IllegalStateException(msg);
            }
        }
        return true;
    }

    private ArgumentConfig getArgumentByIndex(MethodConfig methodConfig, int argIndex) {
        if (methodConfig.getArguments() != null && methodConfig.getArguments().size() > 0) {
            for (ArgumentConfig argument : methodConfig.getArguments()) {
                if (argument.getIndex() != null && argument.getIndex() == argIndex) {
                    return argument;
                }
            }
        }
        return null;
    }

    private boolean hasArgumentConfigProps(Map<String, String> configProperties, String methodName, int argIndex) {
        String argPrefix = methodName + "." + argIndex + ".";
        return ConfigurationUtils.hasSubProperties(configProperties, argPrefix);
    }

    protected MethodConfig getMethodByName(String name) {
        if (methods != null && methods.size() > 0) {
            for (MethodConfig methodConfig : methods) {
                if (StringUtils.isEquals(methodConfig.getName(), name)) {
                    return methodConfig;
                }
            }
        }
        return null;
    }

    /**
     * Legitimacy check of stub, note that: the local will deprecated, and replace with <code>stub</code>
     *
     * @param interfaceClass for provider side, it is the {@link Class} of the service that will be exported; for consumer
     *                       side, it is the {@link Class} of the remote service interface
     */
    protected void checkStubAndLocal(Class<?> interfaceClass) {
        verifyStubAndLocal(local, "Local", interfaceClass);
        verifyStubAndLocal(stub, "Stub", interfaceClass);
    }

    private void verifyStubAndLocal(String className, String label, Class<?> interfaceClass){
        if (ConfigUtils.isNotEmpty(className)) {
            Class<?> localClass = ConfigUtils.isDefault(className) ?
                    ReflectUtils.forName(interfaceClass.getName() + label) : ReflectUtils.forName(className);
                        verify(interfaceClass, localClass);
            }
    }

    private void verify(Class<?> interfaceClass, Class<?> localClass) {
        if (!interfaceClass.isAssignableFrom(localClass)) {
            throw new IllegalStateException("The local implementation class " + localClass.getName() +
                    " not implement interface " + interfaceClass.getName());
        }

        try {
            //Check if the localClass a constructor with parameter who's type is interfaceClass
            ReflectUtils.findConstructor(localClass, interfaceClass);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("No such constructor \"public " + localClass.getSimpleName() +
                    "(" + interfaceClass.getName() + ")\" in local implementation class " + localClass.getName());
        }
    }

    private void convertRegistryIdsToRegistries() {
        computeValidRegistryIds();
        if (StringUtils.isEmpty(registryIds)) {
            if (CollectionUtils.isEmpty(registries)) {
                List<RegistryConfig> registryConfigs = getConfigManager().getDefaultRegistries();
                registryConfigs = new ArrayList<>(registryConfigs);
                setRegistries(registryConfigs);
            }
        } else {
            String[] ids = COMMA_SPLIT_PATTERN.split(registryIds);
            List<RegistryConfig> tmpRegistries = new ArrayList<>();
            Arrays.stream(ids).forEach(id -> {
                if (tmpRegistries.stream().noneMatch(reg -> reg.getId().equals(id))) {
                    Optional<RegistryConfig> globalRegistry = getConfigManager().getRegistry(id);
                    if (globalRegistry.isPresent()) {
                        tmpRegistries.add(globalRegistry.get());
                    } else {
                        throw new IllegalStateException("Registry not found: " + id);
                    }
                }
            });
            setRegistries(tmpRegistries);
        }

    }

    protected boolean notHasSelfRegistryProperty() {
        return CollectionUtils.isEmpty(registries) && StringUtils.isEmpty(registryIds);
    }

    protected void completeCompoundConfigs(AbstractInterfaceConfig interfaceConfig) {
        if (interfaceConfig != null) {
            if (application == null) {
                setApplication(interfaceConfig.getApplication());
            }
            if (module == null) {
                setModule(interfaceConfig.getModule());
            }
            if (notHasSelfRegistryProperty()) {
                setRegistries(interfaceConfig.getRegistries());
                setRegistryIds(interfaceConfig.getRegistryIds());
            }
            if (monitor == null) {
                setMonitor(interfaceConfig.getMonitor());
            }
        }
        if (module != null) {
            if (notHasSelfRegistryProperty()) {
                setRegistries(module.getRegistries());
            }
            if (monitor == null) {
                setMonitor(module.getMonitor());
            }
        }
        if (application != null) {
            if (notHasSelfRegistryProperty()) {
                setRegistries(application.getRegistries());
                setRegistryIds(application.getRegistryIds());
            }
            if (monitor == null) {
                setMonitor(application.getMonitor());
            }
        }
    }
    
    protected void computeValidRegistryIds() {
        if (application != null && notHasSelfRegistryProperty()) {
            setRegistries(application.getRegistries());
            setRegistryIds(application.getRegistryIds());
        }
    }

    /**
     * @return local
     * @deprecated Replace to <code>getStub()</code>
     */
    @Deprecated
    public String getLocal() {
        return local;
    }

    /**
     * @param local
     * @deprecated Replace to <code>setStub(Boolean)</code>
     */
    @Deprecated
    public void setLocal(Boolean local) {
        if (local == null) {
            setLocal((String) null);
        } else {
            setLocal(local.toString());
        }
    }

    /**
     * @param local
     * @deprecated Replace to <code>setStub(String)</code>
     */
    @Deprecated
    public void setLocal(String local) {
        this.local = local;
    }

    public String getStub() {
        return stub;
    }

    public void setStub(Boolean stub) {
        if (stub == null) {
            setStub((String) null);
        } else {
            setStub(stub.toString());
        }
    }

    public void setStub(String stub) {
        this.stub = stub;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getProxy() {
        return proxy;
    }

    public void setProxy(String proxy) {

        this.proxy = proxy;
    }

    public Integer getConnections() {
        return connections;
    }

    public void setConnections(Integer connections) {
        this.connections = connections;
    }

    @Parameter(key = REFERENCE_FILTER_KEY, append = true)
    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    @Parameter(key = INVOKER_LISTENER_KEY, append = true)
    public String getListener() {
        return listener;
    }

    public void setListener(String listener) {
        this.listener = listener;
    }

    public String getLayer() {
        return layer;
    }

    public void setLayer(String layer) {
        this.layer = layer;
    }

    /**
     * Always use the global ApplicationConfig
     */
    public ApplicationConfig getApplication() {
        if (application != null) {
            return application;
        }
        return getConfigManager().getApplicationOrElseThrow();
    }

    /**
     * @deprecated Use {@link AbstractInterfaceConfig#setScopeModel(ScopeModel)}
     * @param application
     */
    @Deprecated
    public void setApplication(ApplicationConfig application) {
        this.application = application;
        if (application != null) {
            getConfigManager().setApplication(application);
        }
    }

    public ModuleConfig getModule() {
        if (module != null) {
            return module;
        }
        return getModuleConfigManager().getModule().orElse(null);
    }

    /**
     * @deprecated Use {@link AbstractInterfaceConfig#setScopeModel(ScopeModel)}
     * @param module
     */
    @Deprecated
    public void setModule(ModuleConfig module) {
        this.module = module;
        if (module != null) {
            getModuleConfigManager().setModule(module);
        }
    }

    public RegistryConfig getRegistry() {
        return CollectionUtils.isEmpty(registries) ? null : registries.get(0);
    }

    public void setRegistry(RegistryConfig registry) {
        List<RegistryConfig> registries = new ArrayList<RegistryConfig>(1);
        registries.add(registry);
        setRegistries(registries);
    }

    public List<RegistryConfig> getRegistries() {
        return registries;
    }

    @SuppressWarnings({"unchecked"})
    public void setRegistries(List<? extends RegistryConfig> registries) {
        this.registries = (List<RegistryConfig>) registries;
    }

    @Parameter(excluded = true)
    public String getRegistryIds() {
        return registryIds;
    }

    public void setRegistryIds(String registryIds) {
        this.registryIds = registryIds;
    }


    public List<MethodConfig> getMethods() {
        return methods;
    }

    @SuppressWarnings("unchecked")
    public void setMethods(List<? extends MethodConfig> methods) {
        this.methods = (methods != null) ? new ArrayList<>(methods) : null;
    }

    public void addMethod(MethodConfig methodConfig) {
        if (this.methods == null) {
            this.methods = new ArrayList<>();
        }
        this.methods.add(methodConfig);
    }

    public MonitorConfig getMonitor() {
        if (monitor != null) {
            return monitor;
        }
        // FIXME: instead of return null, we should set default monitor when getMonitor() return null in ConfigManager
        return getConfigManager().getMonitor().orElse(null);
    }

    /**
     * @deprecated Use {@link org.apache.dubbo.config.context.ConfigManager#setMonitor(MonitorConfig)}
     */
    @Deprecated
    public void setMonitor(String monitor) {
        setMonitor(new MonitorConfig(monitor));
    }

    /**
     * @deprecated Use {@link org.apache.dubbo.config.context.ConfigManager#setMonitor(MonitorConfig)}
     */
    @Deprecated
    public void setMonitor(MonitorConfig monitor) {
        this.monitor = monitor;
        if (monitor != null) {
            getConfigManager().setMonitor(monitor);
        }
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    /**
     * @deprecated Use {@link org.apache.dubbo.config.context.ConfigManager#getConfigCenter(String)}
     */
    @Deprecated
    public ConfigCenterConfig getConfigCenter() {
        if (configCenter != null) {
            return configCenter;
        }
        Collection<ConfigCenterConfig> configCenterConfigs = getConfigManager().getConfigCenters();
        if (CollectionUtils.isNotEmpty(configCenterConfigs)) {
            return configCenterConfigs.iterator().next();
        }
        return null;
    }

    /**
     * @deprecated Use {@link org.apache.dubbo.config.context.ConfigManager#addConfigCenter(ConfigCenterConfig)}
     */
    @Deprecated
    public void setConfigCenter(ConfigCenterConfig configCenter) {
        this.configCenter = configCenter;
        if (configCenter != null) {
            getConfigManager().addConfigCenter(configCenter);
        }
    }

    public Integer getCallbacks() {
        return callbacks;
    }

    public void setCallbacks(Integer callbacks) {
        this.callbacks = callbacks;
    }

    public String getOnconnect() {
        return onconnect;
    }

    public void setOnconnect(String onconnect) {
        this.onconnect = onconnect;
    }

    public String getOndisconnect() {
        return ondisconnect;
    }

    public void setOndisconnect(String ondisconnect) {
        this.ondisconnect = ondisconnect;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    /**
     * @deprecated Use {@link ConfigManager#getMetadataConfigs()}
     */
    @Deprecated
    public MetadataReportConfig getMetadataReportConfig() {
        if (metadataReportConfig != null) {
            return metadataReportConfig;
        }
        Collection<MetadataReportConfig> metadataReportConfigs = getConfigManager().getMetadataConfigs();
        if (CollectionUtils.isNotEmpty(metadataReportConfigs)) {
            return metadataReportConfigs.iterator().next();
        }
        return null;
    }

    /**
     * @deprecated Use {@link ConfigManager#addMetadataReport(MetadataReportConfig)}
     */
    @Deprecated
    public void setMetadataReportConfig(MetadataReportConfig metadataReportConfig) {
        this.metadataReportConfig = metadataReportConfig;
        if (metadataReportConfig != null) {
            getConfigManager().addMetadataReport(metadataReportConfig);
        }
    }

    @Parameter(key = TAG_KEY)
    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public Boolean getAuth() {
        return auth;
    }

    public void setAuth(Boolean auth) {
        this.auth = auth;
    }

    public SslConfig getSslConfig() {
        return getConfigManager().getSsl().orElse(null);
    }
    
    protected void initServiceMetadata(AbstractInterfaceConfig interfaceConfig) {
        serviceMetadata.setVersion(getVersion(interfaceConfig));
        serviceMetadata.setGroup(getGroup(interfaceConfig));
        serviceMetadata.setDefaultGroup(getGroup(interfaceConfig));
        serviceMetadata.setServiceInterfaceName(getInterface());
    }
    
    public String getGroup(AbstractInterfaceConfig interfaceConfig) {
        return StringUtils.isEmpty(getGroup()) ? (interfaceConfig != null ? interfaceConfig.getGroup() : getGroup()) : getGroup();
    }

    public String getVersion(AbstractInterfaceConfig interfaceConfig) {
        return StringUtils.isEmpty(getVersion()) ? (interfaceConfig != null ? interfaceConfig.getVersion() : getVersion()) : getVersion();
    }
    
    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }
    
    public String getInterface() {
        return interfaceName;
    }
    
    public void setInterface(String interfaceName) {
        this.interfaceName = interfaceName;
    }

    public ClassLoader getInterfaceClassLoader() {
        return interfaceClassLoader;
    }

    public void setInterfaceClassLoader(ClassLoader interfaceClassLoader) {
        this.interfaceClassLoader = interfaceClassLoader;
    }
}
