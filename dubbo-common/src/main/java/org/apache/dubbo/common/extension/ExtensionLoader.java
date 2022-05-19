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
package org.apache.dubbo.common.extension;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.beans.support.InstantiationStrategy;
import org.apache.dubbo.common.config.Environment;
import org.apache.dubbo.common.context.ApplicationExt;
import org.apache.dubbo.common.context.Lifecycle;
import org.apache.dubbo.common.extension.support.ActivateComparator;
import org.apache.dubbo.common.extension.support.WrapperComparator;
import org.apache.dubbo.common.lang.Prioritized;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.common.utils.ClassLoaderResourceLoader;
import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.Holder;
import org.apache.dubbo.common.utils.NativeUtils;
import org.apache.dubbo.common.utils.ReflectUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.model.ApplicationModel;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.ModuleModel;
import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.model.ScopeModelAccessor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static java.util.Collections.sort;
import static java.util.ServiceLoader.load;
import static java.util.stream.StreamSupport.stream;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REMOVE_VALUE_PREFIX;

/**
 * {@link org.apache.dubbo.rpc.model.ApplicationModel}, {@code DubboBootstrap} and this class are
 * at present designed to be singleton or static (by itself totally static or uses some static fields).
 * So the instances returned from them are of process or classloader scope. If you want to support
 * multiple dubbo servers in a single process, you may need to refactor these three classes.
 * <p>
 * Load dubbo extensions
 * <ul>
 * <li>auto inject dependency extension </li>
 * <li>auto wrap extension in wrapper </li>
 * <li>default extension is an adaptive instance</li>
 * </ul>
 *
 * @see <a href="http://java.sun.com/j2se/1.5.0/docs/guide/jar/jar.html#Service%20Provider">Service Provider in Java 5</a>
 * @see org.apache.dubbo.common.extension.SPI
 * @see org.apache.dubbo.common.extension.Adaptive
 * @see org.apache.dubbo.common.extension.Activate
 */
public class ExtensionLoader<T> {

    // 这个类就代表了dubbo里的SPI机制的核心点

    private static final Logger logger = LoggerFactory.getLogger(ExtensionLoader.class);

    private static final Pattern NAME_SEPARATOR = Pattern.compile("\\s*[,]+\\s*");

    // 都希望我讲讲，源码里的一些亮点，设计，以及为什么这么做

    // 这是一个缓存，实现类->对象之间的一个缓存
    private final ConcurrentMap<Class<?>, Object> extensionInstances = new ConcurrentHashMap<>(64);
    // 接口class类型
    private final Class<?> type;
    // extension injector，扩展实例注入器
    private final ExtensionInjector injector;
    // 缓存名称
    private final ConcurrentMap<Class<?>, String> cachedNames = new ConcurrentHashMap<>();
    // 缓存类
    private final Holder<Map<String, Class<?>>> cachedClasses = new Holder<>();
    // 缓存的Activate对应的一些东西
    private final Map<String, Object> cachedActivates = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, Set<String>> cachedActivateGroups = Collections.synchronizedMap(new LinkedHashMap<>());
    private final Map<String, String[]> cachedActivateValues = Collections.synchronizedMap(new LinkedHashMap<>());

    // 缓存起来的实例集合
    // 实现name -> extension实例对象的holder容器，缓存映射关系
    private final ConcurrentMap<String, Holder<Object>> cachedInstances = new ConcurrentHashMap<>();
    // 缓存的一些Adaptive东西
    // 自适应，就是说后续要根据实现类比如说@Adaptive注解，要根据一些url参数，自动去筛选和判断到底用哪个实现类
    private final Holder<Object> cachedAdaptiveInstance = new Holder<>();
    private volatile Class<?> cachedAdaptiveClass = null;
    private String cachedDefaultName;
    private volatile Throwable createAdaptiveInstanceError;

    private Set<Class<?>> cachedWrapperClasses;

    private Map<String, IllegalStateException> exceptions = new ConcurrentHashMap<>();

    private static volatile LoadingStrategy[] strategies = loadLoadingStrategies();

    /**
     * Record all unacceptable exceptions when using SPI
     */
    private Set<String> unacceptableExceptions = new ConcurrentHashSet<>();
    private ExtensionDirector extensionDirector;
    private List<ExtensionPostProcessor> extensionPostProcessors;
    private InstantiationStrategy instantiationStrategy;
    private Environment environment;
    private ActivateComparator activateComparator;
    private ScopeModel scopeModel;

    public static void setLoadingStrategies(LoadingStrategy... strategies) {
        if (ArrayUtils.isNotEmpty(strategies)) {
            ExtensionLoader.strategies = strategies;
        }
    }

    /**
     * Load all {@link Prioritized prioritized} {@link LoadingStrategy Loading Strategies} via {@link ServiceLoader}
     *
     * @return non-null
     * @since 2.7.7
     */
    private static LoadingStrategy[] loadLoadingStrategies() {
        return stream(load(LoadingStrategy.class).spliterator(), false)
            .sorted()
            .toArray(LoadingStrategy[]::new);
    }

    /**
     * Get all {@link LoadingStrategy Loading Strategies}
     *
     * @return non-null
     * @see LoadingStrategy
     * @see Prioritized
     * @since 2.7.7
     */
    public static List<LoadingStrategy> getLoadingStrategies() {
        return asList(strategies);
    }

    // 他负责加载实现类的接口class，负责管理他的extension director，跟extension director绑定的scope model
    ExtensionLoader(Class<?> type, ExtensionDirector extensionDirector, ScopeModel scopeModel) {
        this.type = type;
        this.extensionDirector = extensionDirector;
        this.extensionPostProcessors = extensionDirector.getExtensionPostProcessors();
        initInstantiationStrategy(); // 初始化extension实例构建的策略逻辑
        // 他自己就用了SPI机制，获取到了extension injector
        // injector，注入器，依赖注入，extensino实例构建的过程中，是有一个依赖注入的过程的
        this.injector = (type == ExtensionInjector.class ? null : extensionDirector.getExtensionLoader(ExtensionInjector.class)
            .getAdaptiveExtension());
        // 自动激活的比较组件
        this.activateComparator = new ActivateComparator(extensionDirector);
        this.scopeModel = scopeModel;
    }

    private void initInstantiationStrategy() {
        for (ExtensionPostProcessor extensionPostProcessor : extensionPostProcessors) {
            if (extensionPostProcessor instanceof ScopeModelAccessor) {
                instantiationStrategy = new InstantiationStrategy((ScopeModelAccessor) extensionPostProcessor);
                break;
            }
        }
        if (instantiationStrategy == null) {
            instantiationStrategy = new InstantiationStrategy();
        }
    }

    /**
     * @see ApplicationModel#getExtensionDirector()
     * @see FrameworkModel#getExtensionDirector()
     * @see ModuleModel#getExtensionDirector()
     * @see ExtensionDirector#getExtensionLoader(java.lang.Class)
     * @deprecated get extension loader from extension director of some module.
     */
    @Deprecated
    public static <T> ExtensionLoader<T> getExtensionLoader(Class<T> type) {
        return ApplicationModel.defaultModel().getDefaultModule().getExtensionLoader(type);
    }

    // For testing purposes only
    public static void resetExtensionLoader(Class type) {
//        ExtensionLoader loader = EXTENSION_LOADERS.get(type);
//        if (loader != null) {
//            // Remove all instances associated with this loader as well
//            Map<String, Class<?>> classes = loader.getExtensionClasses();
//            for (Map.Entry<String, Class<?>> entry : classes.entrySet()) {
//                EXTENSION_INSTANCES.remove(entry.getValue());
//            }
//            classes.clear();
//            EXTENSION_LOADERS.remove(type);
//        }
    }

    public void destroy() {
        extensionInstances.forEach((type, instance) -> {
            if (instance instanceof Lifecycle) {
                Lifecycle lifecycle = (Lifecycle) instance;
                try {
                    lifecycle.destroy();
                } catch (Exception e) {
                    logger.error("Error destroying extension " + lifecycle, e);
                }
            }
        });
        extensionInstances.clear();

        // TODO destroy extension loader, release resources.
    }

    private static ClassLoader findClassLoader() {
        return ClassUtils.getClassLoader(ExtensionLoader.class);
    }

    public String getExtensionName(T extensionInstance) {
        return getExtensionName(extensionInstance.getClass());
    }

    public String getExtensionName(Class<?> extensionClass) {
        getExtensionClasses();// load class
        return cachedNames.get(extensionClass);
    }

    /**
     * This is equivalent to {@code getActivateExtension(url, key, null)}
     *
     * @param url url
     * @param key url parameter key which used to get extension point names
     * @return extension list which are activated.
     * @see #getActivateExtension(org.apache.dubbo.common.URL, String, String)
     */
    public List<T> getActivateExtension(URL url, String key) {
        return getActivateExtension(url, key, null);
    }

    /**
     * This is equivalent to {@code getActivateExtension(url, values, null)}
     *
     * @param url    url
     * @param values extension point names
     * @return extension list which are activated
     * @see #getActivateExtension(org.apache.dubbo.common.URL, String[], String)
     */
    public List<T> getActivateExtension(URL url, String[] values) {
        return getActivateExtension(url, values, null);
    }

    /**
     * This is equivalent to {@code getActivateExtension(url, url.getParameter(key).split(","), null)}
     *
     * @param url   url
     * @param key   url parameter key which used to get extension point names
     * @param group group
     * @return extension list which are activated.
     * @see #getActivateExtension(org.apache.dubbo.common.URL, String[], String)
     */
    public List<T> getActivateExtension(URL url, String key, String group) {
        String value = url.getParameter(key);
        return getActivateExtension(url, StringUtils.isEmpty(value) ? null : COMMA_SPLIT_PATTERN.split(value), group);
    }

    /**
     * Get activate extensions.
     *
     * @param url    url
     * @param values extension point names
     * @param group  group
     * @return extension list which are activated
     * @see org.apache.dubbo.common.extension.Activate
     */
    public List<T> getActivateExtension(URL url, String[] values, String group) {
        // dubbo SPI机制
        // 扩展性的思考，如果让你来设计一款框架，干很多的事情，任何一个框架都是需要有扩展机制
        // 扩展机制，你会如何来设计呢？
        // 一般来说很多其他的框架，都有自己的扩展机制，在配置文件里，可以做一些配置项
        // xx.xx.xx.Class = com.zhss.dd.xx.xMyClass
        // 框架在运行的时候，就会加载到这个配置，然后就会把你提供的类加载进来，作为具体的实现就可以了
        // 对于一个框架运行过程中，一般就开放少数的一些组件可以让你去进行定制
        // SPI机制，做的就比较彻底了，大量的核心组件，都是可以替换，让整个框架的扩展性和定制性
        // 如果我们就是一些业务系统的话，一般来说还不需要用到dubbo的SPI扩展机制

        // 根据你的@Activate注解，对你的一个接口可以自动激活多个实现类

        // solve the bug of using @SPI's wrapper method to report a null pointer exception.
        Map<Class<?>, T> activateExtensionsMap = new TreeMap<>(activateComparator);
        List<String> names = values == null ? new ArrayList<>(0) : asList(values);
        Set<String> namesSet = new HashSet<>(names);

        if (!namesSet.contains(REMOVE_VALUE_PREFIX + DEFAULT_KEY)) {
            if (cachedActivateGroups.size() == 0) {
                synchronized (cachedActivateGroups) {
                    // cache all extensions
                    if (cachedActivateGroups.size() == 0) {
                        getExtensionClasses(); // 都会基于配置文件去加载你的接口所有的实现类
                        // 所有实现类如果加了@Activate注解，此时这些注解都会被缓存起来

                        // 在这里，他核心是要处理被缓存起来的@Activate一个一个的注解
                        // 所有实现类的@Activate注解都会被遍历和处理
                        for (Map.Entry<String, Object> entry : cachedActivates.entrySet()) {
                            // 拿到缓存好的名称和一个自动激活的对象实例（@Activate注解）
                            String name = entry.getKey();
                            Object activate = entry.getValue();

                            String[] activateGroup, activateValue;

                            if (activate instanceof Activate) {
                                // 就会从@Activate注解里提取出来对应的group和value两个参数
                                activateGroup = ((Activate) activate).group();
                                activateValue = ((Activate) activate).value();
                            } else if (activate instanceof com.alibaba.dubbo.common.extension.Activate) {
                                activateGroup = ((com.alibaba.dubbo.common.extension.Activate) activate).group();
                                activateValue = ((com.alibaba.dubbo.common.extension.Activate) activate).value();
                            } else {
                                continue;
                            }

                            // 就可以把提取出来的group和value的参数
                            // 你的每个实现类的短名称和他的@Activate注解的缓存
                            cachedActivateGroups.put(name, new HashSet<>(Arrays.asList(activateGroup)));
                            cachedActivateValues.put(name, activateValue);
                        }
                    }
                }
            }

            // traverse all cached extensions
            cachedActivateGroups.forEach((name, activateGroup) -> {
                if (isMatchGroup(group, activateGroup)
                    && !namesSet.contains(name)
                    && !namesSet.contains(REMOVE_VALUE_PREFIX + name)
                    && isActive(cachedActivateValues.get(name), url)) {

                    // 根据你的name，去获取到每个name对应的extension class，还有获取一个extension实例
                    // 凡是打了@Activate注解的实现类的短名称，在这里都会被加载和初始化
                    activateExtensionsMap.put(getExtensionClass(name), getExtension(name));
                }
            });
        }

        if (namesSet.contains(DEFAULT_KEY)) {
            // will affect order
            // `ext1,default,ext2` means ext1 will happens before all of the default extensions while ext2 will after them
            ArrayList<T> extensionsResult = new ArrayList<>(activateExtensionsMap.size() + names.size());
            for (int i = 0; i < names.size(); i++) {
                String name = names.get(i);
                if (!name.startsWith(REMOVE_VALUE_PREFIX)
                    && !namesSet.contains(REMOVE_VALUE_PREFIX + name)) {
                    if (!DEFAULT_KEY.equals(name)) {
                        if (containsExtension(name)) {
                            extensionsResult.add(getExtension(name));
                        }
                    } else {
                        extensionsResult.addAll(activateExtensionsMap.values());
                    }
                }
            }

            // 就是根据你传递进来的那些东西，去返回一个result结果list
            return extensionsResult;
        } else {
            // add extensions, will be sorted by its order
            for (int i = 0; i < names.size(); i++) {
                String name = names.get(i);
                if (!name.startsWith(REMOVE_VALUE_PREFIX)
                    && !namesSet.contains(REMOVE_VALUE_PREFIX + name)) {
                    if (!DEFAULT_KEY.equals(name)) {
                        if (containsExtension(name)) {
                            activateExtensionsMap.put(getExtensionClass(name), getExtension(name));
                        }
                    }
                }
            }
            return new ArrayList<>(activateExtensionsMap.values());
        }
    }

    public List<T> getActivateExtensions() {
        List<T> activateExtensions = new ArrayList<>();
        TreeMap<Class<?>, T> activateExtensionsMap = new TreeMap<>(activateComparator);
        getExtensionClasses();
        for (Map.Entry<String, Object> entry : cachedActivates.entrySet()) {
            String name = entry.getKey();
            Object activate = entry.getValue();
            if (!(activate instanceof Activate)) {
                continue;
            }
            activateExtensionsMap.put(getExtensionClass(name), getExtension(name));
        }
        if (!activateExtensionsMap.isEmpty()) {
            activateExtensions.addAll(activateExtensionsMap.values());
        }

        return activateExtensions;
    }

    private boolean isMatchGroup(String group, Set<String> groups) {
        if (StringUtils.isEmpty(group)) {
            return true;
        }
        if (CollectionUtils.isNotEmpty(groups)) {
            return groups.contains(group);
        }
        return false;
    }

    private boolean isActive(String[] keys, URL url) {
        if (keys.length == 0) {
            return true;
        }
        for (String key : keys) {
            // @Active(value="key1:value1, key2:value2")
            String keyValue = null;
            if (key.contains(":")) {
                String[] arr = key.split(":");
                key = arr[0];
                keyValue = arr[1];
            }

            String realValue = url.getParameter(key);
            if (StringUtils.isEmpty(realValue)) {
                realValue = url.getAnyMethodParameter(key);
            }
            if ((keyValue != null && keyValue.equals(realValue)) || (keyValue == null && ConfigUtils.isNotEmpty(realValue))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get extension's instance. Return <code>null</code> if extension is not found or is not initialized. Pls. note
     * that this method will not trigger extension load.
     * <p>
     * In order to trigger extension load, call {@link #getExtension(String)} instead.
     *
     * @see #getExtension(String)
     */
    @SuppressWarnings("unchecked")
    public T getLoadedExtension(String name) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("Extension name == null");
        }
        Holder<Object> holder = getOrCreateHolder(name);
        return (T) holder.get();
    }

    private Holder<Object> getOrCreateHolder(String name) {
        // 根据你的name名称，先去获取一下Holder
        Holder<Object> holder = cachedInstances.get(name);
        if (holder == null) {
            // 构建一个空的Holder，放进缓存里就可以了
            cachedInstances.putIfAbsent(name, new Holder<>());
            holder = cachedInstances.get(name);
        }
        return holder;
    }

    /**
     * Return the list of extensions which are already loaded.
     * <p>
     * Usually {@link #getSupportedExtensions()} should be called in order to get all extensions.
     *
     * @see #getSupportedExtensions()
     */
    public Set<String> getLoadedExtensions() {
        return Collections.unmodifiableSet(new TreeSet<>(cachedInstances.keySet()));
    }

    public List<T> getLoadedExtensionInstances() {
        List<T> instances = new ArrayList<>();
        cachedInstances.values().forEach(holder -> instances.add((T) holder.get()));
        return instances;
    }

    public Object getLoadedAdaptiveExtensionInstances() {
        return cachedAdaptiveInstance.get();
    }

    /**
     * Find the extension with the given name. If the specified name is not found, then {@link IllegalStateException}
     * will be thrown.
     */
    @SuppressWarnings("unchecked")
    public T getExtension(String name) {
        // dubbo源码里
        // extensionDirector.getExtensionLoader(Class).getExtension()
        // name一般是从你的SPI注解里提取出来的一个name
        T extension = getExtension(name, true); // wrap参数默认是true
        if (extension == null) {
            throw new IllegalArgumentException("Not find extension: " + name);
        }
        return extension;
    }

    public T getExtension(String name, boolean wrap) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("Extension name == null");
        }
        if ("true".equals(name)) {
            return getDefaultExtension();
        }

        // 做了一个cache key，缓存key
        String cacheKey = name;
        if (!wrap) {
            cacheKey += "_origin";
        }

        // 他去基于这个cache key，缓存key构建和创建了一个Holder容器
        // 刚开始是找不到对应的缓存的，只能先针对name，cachekey构建一个holder
        final Holder<Object> holder = getOrCreateHolder(cacheKey);

        // dubbo源码里设计的一个细节
        // 针对spi extension实例进行构建的细粒度加锁的机制

        // 对holder走了一个get，拿到了里面具体封装的一个实现类的对象
        Object instance = holder.get();
        // 如果是空，那么就需要去构建这个实现类的对象，一般会走一个多线程的double check
        if (instance == null) {
            synchronized (holder) {
                instance = holder.get();
                if (instance == null) {
                    // 创建一个对象
                    instance = createExtension(name, wrap);
                    // 把创建出来的实现类的对象，设置到holder里去
                    holder.set(instance);
                }
            }
        }

        return (T) instance;
    }

    /**
     * Get the extension by specified name if found, or {@link #getDefaultExtension() returns the default one}
     *
     * @param name the name of extension
     * @return non-null
     */
    public T getOrDefaultExtension(String name) {
        return containsExtension(name) ? getExtension(name) : getDefaultExtension();
    }

    /**
     * Return default extension, return <code>null</code> if it's not configured.
     */
    public T getDefaultExtension() {
        // 就是根据SPI的规范，去读取配置，加载你的实现类
        getExtensionClasses();
        if (StringUtils.isBlank(cachedDefaultName) || "true".equals(cachedDefaultName)) {
            return null;
        }
        // 加载完毕过后，此时就可以根据缓存默认名称，去获取你需要的一个实现类就可以了
        return getExtension(cachedDefaultName);
    }

    public boolean hasExtension(String name) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("Extension name == null");
        }
        Class<?> c = this.getExtensionClass(name);
        return c != null;
    }

    public Set<String> getSupportedExtensions() {
        Map<String, Class<?>> clazzes = getExtensionClasses();
        return Collections.unmodifiableSet(new TreeSet<>(clazzes.keySet()));
    }

    public Set<T> getSupportedExtensionInstances() {
        List<T> instances = new LinkedList<>();
        Set<String> supportedExtensions = getSupportedExtensions();
        if (CollectionUtils.isNotEmpty(supportedExtensions)) {
            for (String name : supportedExtensions) {
                instances.add(getExtension(name));
            }
        }
        // sort the Prioritized instances
        sort(instances, Prioritized.COMPARATOR);
        return new LinkedHashSet<>(instances);
    }

    /**
     * Return default extension name, return <code>null</code> if not configured.
     */
    public String getDefaultExtensionName() {
        getExtensionClasses();
        return cachedDefaultName;
    }

    /**
     * Register new extension via API
     *
     * @param name  extension name
     * @param clazz extension class
     * @throws IllegalStateException when extension with the same name has already been registered.
     */
    public void addExtension(String name, Class<?> clazz) {
        getExtensionClasses(); // load classes

        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Input type " +
                clazz + " doesn't implement the Extension " + type);
        }
        if (clazz.isInterface()) {
            throw new IllegalStateException("Input type " +
                clazz + " can't be interface!");
        }

        if (!clazz.isAnnotationPresent(Adaptive.class)) {
            if (StringUtils.isBlank(name)) {
                throw new IllegalStateException("Extension name is blank (Extension " + type + ")!");
            }
            if (cachedClasses.get().containsKey(name)) {
                throw new IllegalStateException("Extension name " +
                    name + " already exists (Extension " + type + ")!");
            }

            cachedNames.put(clazz, name);
            cachedClasses.get().put(name, clazz);
        } else {
            if (cachedAdaptiveClass != null) {
                throw new IllegalStateException("Adaptive Extension already exists (Extension " + type + ")!");
            }

            cachedAdaptiveClass = clazz;
        }
    }

    /**
     * Replace the existing extension via API
     *
     * @param name  extension name
     * @param clazz extension class
     * @throws IllegalStateException when extension to be placed doesn't exist
     * @deprecated not recommended any longer, and use only when test
     */
    @Deprecated
    public void replaceExtension(String name, Class<?> clazz) {
        getExtensionClasses(); // load classes

        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Input type " +
                clazz + " doesn't implement Extension " + type);
        }
        if (clazz.isInterface()) {
            throw new IllegalStateException("Input type " +
                clazz + " can't be interface!");
        }

        if (!clazz.isAnnotationPresent(Adaptive.class)) {
            if (StringUtils.isBlank(name)) {
                throw new IllegalStateException("Extension name is blank (Extension " + type + ")!");
            }
            if (!cachedClasses.get().containsKey(name)) {
                throw new IllegalStateException("Extension name " +
                    name + " doesn't exist (Extension " + type + ")!");
            }

            cachedNames.put(clazz, name);
            cachedClasses.get().put(name, clazz);
            cachedInstances.remove(name);
        } else {
            if (cachedAdaptiveClass == null) {
                throw new IllegalStateException("Adaptive Extension doesn't exist (Extension " + type + ")!");
            }

            cachedAdaptiveClass = clazz;
            cachedAdaptiveInstance.set(null);
        }
    }

    @SuppressWarnings("unchecked")
    public T getAdaptiveExtension() {
        // 针对@Adaptive注解的SPI扩展机制，如果你有多个实现类，可以根据url里带的一些参数
        // 直接就是可以匹配和定位对应的一个实现类
        // 他在这里动态生成出来的一个类，不是说直接给我们用的实现类，根据我们动态加载的一些需要和需求
        // 生成出来的这个类，核心要做的事情，根据url里提取出的参数，动态匹配你的实现类


        Object instance = cachedAdaptiveInstance.get();
        if (instance == null) {
            if (createAdaptiveInstanceError != null) {
                throw new IllegalStateException("Failed to create adaptive instance: " +
                    createAdaptiveInstanceError.toString(),
                    createAdaptiveInstanceError);
            }

            // 为什么要有这个holder，holder是一个编码的设计，很大程度上而言是用来对你的目标对象进行加锁的
            synchronized (cachedAdaptiveInstance) {
                instance = cachedAdaptiveInstance.get();
                if (instance == null) {
                    try {
                        instance = createAdaptiveExtension();
                        cachedAdaptiveInstance.set(instance);
                    } catch (Throwable t) {
                        createAdaptiveInstanceError = t;
                        throw new IllegalStateException("Failed to create adaptive instance: " + t.toString(), t);
                    }
                }
            }
        }

        return (T) instance;
    }

    private IllegalStateException findException(String name) {
        StringBuilder buf = new StringBuilder("No such extension " + type.getName() + " by name " + name);

        int i = 1;
        for (Map.Entry<String, IllegalStateException> entry : exceptions.entrySet()) {
            if (entry.getKey().toLowerCase().startsWith(name.toLowerCase())) {
                if (i == 1) {
                    buf.append(", possible causes: ");
                }
                buf.append("\r\n(");
                buf.append(i++);
                buf.append(") ");
                buf.append(entry.getKey());
                buf.append(":\r\n");
                buf.append(StringUtils.toString(entry.getValue()));
            }
        }

        if (i == 1) {
            buf.append(", no related exception was found, please check whether related SPI module is missing.");
        }
        return new IllegalStateException(buf.toString());
    }

    @SuppressWarnings("unchecked")
    private T createExtension(String name, boolean wrap) {

        // 根据这个名称，name，从你加载出来的那些实现类里拿出来一个具体的Class
        // 根据接口拿到实现类，构建实现类的对象，注入给接口类型的变量
        // 通过指定的配置文件读取和解析，先去拿到extension classes，通过实现类的短名称，去获取到对应的class对象
        Class<?> clazz = getExtensionClasses().get(name);
        if (clazz == null || unacceptableExceptions.contains(name)) {
            throw findException(name);
        }
        try {
            // 先去获取一下缓存的对象
            T instance = (T) extensionInstances.get(clazz);
            if (instance == null) {
                // 直接会去构建一个实现类的对象，放到缓存里去
                // 核心就是对实现类，通过反射技术，拿到一个对应的实现类的对象实例
                extensionInstances.putIfAbsent(clazz, createExtensionInstance(clazz));
                instance = (T) extensionInstances.get(clazz);
                // 去做一个具体对这个实例对象进行初始化之前的一个post process
                instance = postProcessBeforeInitialization(instance, name);
                // 依赖入驻，dubbo SPI机制，本身扮演了类似spring容器的机制，他会托管你的实现类的对象
                injectExtension(instance);
                // 去做一个具体对这个实例对象进行初始化之后的一个post process
                instance = postProcessAfterInitialization(instance, name);
            }

            // 如果说要对这个实现类的实例进行包装，默认就是true
            if (wrap) {
                List<Class<?>> wrapperClassesList = new ArrayList<>();
                if (cachedWrapperClasses != null) {
                    wrapperClassesList.addAll(cachedWrapperClasses);
                    wrapperClassesList.sort(WrapperComparator.COMPARATOR);
                    Collections.reverse(wrapperClassesList);
                }

                if (CollectionUtils.isNotEmpty(wrapperClassesList)) {
                    for (Class<?> wrapperClass : wrapperClassesList) {
                        // 如果你的wrapper class有Wrapper注解
                        Wrapper wrapper = wrapperClass.getAnnotation(Wrapper.class);
                        if (wrapper == null
                            || (ArrayUtils.contains(wrapper.matches(), name) && !ArrayUtils.contains(wrapper.mismatches(), name))) {
                            instance = injectExtension((T) wrapperClass.getConstructor(type).newInstance(instance));
                            instance = postProcessAfterInitialization(instance, name);
                        }
                    }
                }
            }

            // Warning: After an instance of Lifecycle is wrapped by cachedWrapperClasses, it may not still be Lifecycle instance, this application may not invoke the lifecycle.initialize hook.
            initExtension(instance);
            return instance;
        } catch (Throwable t) {
            throw new IllegalStateException("Extension instance (name: " + name + ", class: " +
                type + ") couldn't be instantiated: " + t.getMessage(), t);
        }
    }

    private Object createExtensionInstance(Class<?> type) throws ReflectiveOperationException {
        // 就会用我们之前看到的那个实例化的策略逻辑，对这个类进行实例化
        return instantiationStrategy.instantiate(type);
    }

    private T postProcessBeforeInitialization(T instance, String name) throws Exception {
        if (extensionPostProcessors != null) {
            for (ExtensionPostProcessor processor : extensionPostProcessors) {
                instance = (T) processor.postProcessBeforeInitialization(instance, name);
            }
        }
        return instance;
    }

    private T postProcessAfterInitialization(T instance, String name) throws Exception {
        if (instance instanceof ExtensionAccessorAware) {
            ((ExtensionAccessorAware) instance).setExtensionAccessor(extensionDirector);
        }
        if (extensionPostProcessors != null) {
            for (ExtensionPostProcessor processor : extensionPostProcessors) {
                instance = (T) processor.postProcessAfterInitialization(instance, name);
            }
        }
        return instance;
    }

    private boolean containsExtension(String name) {
        return getExtensionClasses().containsKey(name);
    }

    private T injectExtension(T instance) {

        if (injector == null) {
            return instance;
        }

        try {
            // RegistryProtocol实例化好了之后，此时就应该会把其他的实例注入进去
            for (Method method : instance.getClass().getMethods()) {
                if (!isSetter(method)) {
                    continue;
                }
                /**
                 * Check {@link DisableInject} to see if we need auto injection for this property
                 */
                if (method.getAnnotation(DisableInject.class) != null) {
                    continue;
                }

                // 基于这个setter方法去进行依赖注入，会找到对应的入参类型，Protocol
                Class<?> pt = method.getParameterTypes()[0];
                if (ReflectUtils.isPrimitives(pt)) {
                    continue;
                }

                try {
                    // 会去获取到你的setter属性，你要设置的属性值，protocol
                    String property = getSetterProperty(method);
                    // 他会在这里，去获取到，比如说，如果你注入的这个对象是其他的SPI机制里的实现类的对象
                    // 此时就直接从容器里进行获取，注入给你就可以了
                    Object object = injector.getInstance(pt, property);
                    if (object != null) {
                        // 调用setter方法去实现一个依赖注入
                        method.invoke(instance, object);
                    }
                } catch (Exception e) {
                    logger.error("Failed to inject via method " + method.getName()
                        + " of interface " + type.getName() + ": " + e.getMessage(), e);
                }

            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        return instance;
    }

    private void initExtension(T instance) {
        if (instance instanceof Lifecycle) {
            // 你的实现类一般来说可以去实现一个Lifecycle接口
            Lifecycle lifecycle = (Lifecycle) instance;
            lifecycle.initialize(); // 这个方法是留给你自己去实现了
        }
    }

    /**
     * get properties name for setter, for instance: setVersion, return "version"
     * <p>
     * return "", if setter name with length less than 3
     */
    private String getSetterProperty(Method method) {
        return method.getName().length() > 3 ? method.getName().substring(3, 4).toLowerCase() + method.getName().substring(4) : "";
    }

    /**
     * return true if and only if:
     * <p>
     * 1, public
     * <p>
     * 2, name starts with "set"
     * <p>
     * 3, only has one parameter
     */
    private boolean isSetter(Method method) {
        return method.getName().startsWith("set")
            && method.getParameterTypes().length == 1
            && Modifier.isPublic(method.getModifiers());
    }

    private Class<?> getExtensionClass(String name) {
        if (type == null) {
            throw new IllegalArgumentException("Extension type == null");
        }
        if (name == null) {
            throw new IllegalArgumentException("Extension name == null");
        }
        return getExtensionClasses().get(name);
    }

    private Map<String, Class<?>> getExtensionClasses() {
        // 有一个cached classes，也是一个缓存的集合
        // 实现类的短名称 -> 实现类的Class，映射关系，cached classes放到一个holder里去
        Map<String, Class<?>> classes = cachedClasses.get();
        if (classes == null) {
            synchronized (cachedClasses) {
                classes = cachedClasses.get();
                if (classes == null) {
                    // 做具体的一个load，他其实就是要去寻找你对应的extension classes
                    classes = loadExtensionClasses();
                    cachedClasses.set(classes);
                }
            }
        }
        return classes;
    }

    /**
     * synchronized in getExtensionClasses
     */
    private Map<String, Class<?>> loadExtensionClasses() {
        // 缓存默认的扩展名称
        cacheDefaultExtensionName();
        // 扩展类的集合，短名称->实现类Class的映射关系
        // 我的这个接口对应的所有的实现类，都有哪些，每个实现类都有自己的class类型，name短名称
        Map<String, Class<?>> extensionClasses = new HashMap<>();

        for (LoadingStrategy strategy : strategies) {
            // 读取指定目录的下的接口的配置文件，解析配置文件，读取出每个实现类的短名称和class类型
            loadDirectory(extensionClasses, strategy, type.getName());

            // compatible with old ExtensionFactory
            if (this.type == ExtensionInjector.class) {
                loadDirectory(extensionClasses, strategy, ExtensionFactory.class.getName());
            }
        }

        return extensionClasses;
    }

    private void loadDirectory(Map<String, Class<?>> extensionClasses, LoadingStrategy strategy, String type) {
        // 这里是针对的常规的一些类去进行装载
        loadDirectory(extensionClasses, strategy.directory(), type, strategy.preferExtensionClassLoader(),
            strategy.overridden(), strategy.excludedPackages(), strategy.onlyExtensionClassLoaderPackages());
        String oldType = type.replace("org.apache", "com.alibaba");
        // 你的接口里的org.apache替换为了com.alibaba
        loadDirectory(extensionClasses, strategy.directory(), oldType, strategy.preferExtensionClassLoader(),
            strategy.overridden(), strategy.excludedPackages(), strategy.onlyExtensionClassLoaderPackages());
    }

    /**
     * extract and cache default extension name if exists
     */
    private void cacheDefaultExtensionName() {
        // 根据你的type，接口，拿到对应的SPI注解
        final SPI defaultAnnotation = type.getAnnotation(SPI.class);
        if (defaultAnnotation == null) {
            return;
        }

        // 拿到注解里的值，实现类的短名称
        String value = defaultAnnotation.value();
        if ((value = value.trim()).length() > 0) {
            String[] names = NAME_SEPARATOR.split(value);
            if (names.length > 1) {
                throw new IllegalStateException("More than 1 default extension name on extension " + type.getName()
                    + ": " + Arrays.toString(names));
            }
            if (names.length == 1) {
                // 会把你的接口的@SPI注解的短名称，做一个缓存
                cachedDefaultName = names[0];
            }
        }
    }

    private void loadDirectory(Map<String, Class<?>> extensionClasses, String dir, String type) {
        loadDirectory(extensionClasses, dir, type, false, false, new String[]{}, new String[]{});
    }

    private void loadDirectory(Map<String, Class<?>> extensionClasses, String dir, String type,
                               boolean extensionLoaderClassLoaderFirst, boolean overridden,
                               String[] excludedPackages, String[] onlyExtensionClassLoaderPackages) {
        // dir是目录，type是接口的名字
        // 是不是就跟我们之前讲过的，你的dubbo SPI配置的规则是匹配上的
        String fileName = dir + type;
        try {
            List<ClassLoader> classLoadersToLoad = new LinkedList<>();

            // try to load from ExtensionLoader's ClassLoader first
            if (extensionLoaderClassLoaderFirst) {
                ClassLoader extensionLoaderClassLoader = ExtensionLoader.class.getClassLoader();
                if (ClassLoader.getSystemClassLoader() != extensionLoaderClassLoader) {
                    classLoadersToLoad.add(extensionLoaderClassLoader);
                }
            }

            // load from scope model
            Set<ClassLoader> classLoaders = scopeModel.getClassLoaders();

            if (CollectionUtils.isEmpty(classLoaders)) {
                // ClassLoader去加载资源的方法，基于我们的文件名，拿出来了资源集合
                Enumeration<java.net.URL> resources = ClassLoader.getSystemResources(fileName);
                if (resources != null) {
                    // 对这个里面的资源进行了一个遍历
                    while (resources.hasMoreElements()) {
                        loadResource(extensionClasses, null, resources.nextElement(), overridden, excludedPackages, onlyExtensionClassLoaderPackages);
                    }
                }
            } else {
                classLoadersToLoad.addAll(classLoaders);
            }

            // 这个步骤和过程跟上面讲的是差不多的，都是基于规则去读取各种配置文件
            // 拿到对应的实现类，以及进行资源的加载，extension class的加载
            Map<ClassLoader, Set<java.net.URL>> resources = ClassLoaderResourceLoader.loadResources(fileName, classLoadersToLoad);
            resources.forEach(((classLoader, urls) -> {
                loadFromClass(extensionClasses, overridden, urls, classLoader, excludedPackages, onlyExtensionClassLoaderPackages);
            }));
        } catch (Throwable t) {
            logger.error("Exception occurred when loading extension class (interface: " +
                type + ", description file: " + fileName + ").", t);
        }
    }

    private void loadFromClass(Map<String, Class<?>> extensionClasses, boolean overridden, Set<java.net.URL> urls, ClassLoader classLoader,
                               String[] excludedPackages, String[] onlyExtensionClassLoaderPackages) {
        if (CollectionUtils.isNotEmpty(urls)) {
            for (java.net.URL url : urls) {
                loadResource(extensionClasses, classLoader, url, overridden, excludedPackages, onlyExtensionClassLoaderPackages);
            }
        }
    }

    private void loadResource(Map<String, Class<?>> extensionClasses, ClassLoader classLoader,
                              java.net.URL resourceURL, boolean overridden, String[] excludedPackages, String[] onlyExtensionClassLoaderPackages) {
        // 直接就是基于你的配置文件，规则，io读取和加载，就会把你的extension class加载出来
        // 对你的@Adaptive、@Activate不同的机制的类都会去做分别的处理和缓存

        try {
            // 典型的io操作了
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(resourceURL.openStream(), StandardCharsets.UTF_8))) {
                String line;
                String clazz = null;
                // io操作，一行一行的读取
                while ((line = reader.readLine()) != null) {
                    final int ci = line.indexOf('#');
                    if (ci >= 0) {
                        line = line.substring(0, ci);
                    }
                    line = line.trim();
                    if (line.length() > 0) {
                        try {
                            String name = null;
                            int i = line.indexOf('=');
                            if (i > 0) {
                                // 在这里对读出来的一行一行的数据，都会去处理成类名
                                name = line.substring(0, i).trim();
                                clazz = line.substring(i + 1).trim();
                            } else {
                                clazz = line;
                            }
                            if (StringUtils.isNotEmpty(clazz) && !isExcluded(clazz, excludedPackages)
                                && !isExcludedByClassLoader(clazz, classLoader, onlyExtensionClassLoaderPackages)) {
                                loadClass(extensionClasses, resourceURL, Class.forName(clazz, true, classLoader), name, overridden);
                            }
                        } catch (Throwable t) {
                            IllegalStateException e = new IllegalStateException("Failed to load extension class (interface: " + type +
                                ", class line: " + line + ") in " + resourceURL + ", cause: " + t.getMessage(), t);
                            exceptions.put(line, e);
                        }
                    }
                }
            }
        } catch (Throwable t) {
            logger.error("Exception occurred when loading extension class (interface: " +
                type + ", class file: " + resourceURL + ") in " + resourceURL, t);
        }
    }

    private boolean isExcluded(String className, String... excludedPackages) {
        if (excludedPackages != null) {
            for (String excludePackage : excludedPackages) {
                if (className.startsWith(excludePackage + ".")) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isExcludedByClassLoader(String className, ClassLoader classLoader, String... onlyExtensionClassLoaderPackages) {
        if (onlyExtensionClassLoaderPackages != null) {
            for (String excludePackage : onlyExtensionClassLoaderPackages) {
                if (className.startsWith(excludePackage + ".")) {
                    // if target classLoader is not ExtensionLoader's classLoader should be excluded
                    return !Objects.equals(ExtensionLoader.class.getClassLoader(), classLoader);
                }
            }
        }
        return false;
    }

    private void loadClass(Map<String, Class<?>> extensionClasses, java.net.URL resourceURL, Class<?> clazz, String name,
                           boolean overridden) throws NoSuchMethodException {
        if (!type.isAssignableFrom(clazz)) {
            throw new IllegalStateException("Error occurred when loading extension class (interface: " +
                type + ", class line: " + clazz.getName() + "), class "
                + clazz.getName() + " is not subtype of interface.");
        }
        // 你呢其实是可以给类@Adaptive注解
        // 每次SPI机制运行的时候，都会去通过接口的配置文件，加载你的实现类
        if (clazz.isAnnotationPresent(Adaptive.class)) {
            cacheAdaptiveClass(clazz, overridden);
        } else if (isWrapperClass(clazz)) {
            cacheWrapperClass(clazz);
        } else {
            if (StringUtils.isEmpty(name)) {
                name = findAnnotationName(clazz);
                if (name.length() == 0) {
                    throw new IllegalStateException("No such extension name for the class " + clazz.getName() + " in the config " + resourceURL);
                }
            }

            String[] names = NAME_SEPARATOR.split(name);
            if (ArrayUtils.isNotEmpty(names)) {
                // 会对@Activate，自动激活的类做一些缓存和处理
                cacheActivateClass(clazz, names[0]);
                for (String n : names) {
                    cacheName(clazz, n);
                    saveInExtensionClass(extensionClasses, clazz, n, overridden);
                }
            }
        }
    }

    /**
     * cache name
     */
    private void cacheName(Class<?> clazz, String name) {
        if (!cachedNames.containsKey(clazz)) {
            cachedNames.put(clazz, name);
        }
    }

    /**
     * put clazz in extensionClasses
     */
    private void saveInExtensionClass(Map<String, Class<?>> extensionClasses, Class<?> clazz, String name, boolean overridden) {
        Class<?> c = extensionClasses.get(name);
        if (c == null || overridden) {
            extensionClasses.put(name, clazz);
        } else if (c != clazz) {
            // duplicate implementation is unacceptable
            unacceptableExceptions.add(name);
            String duplicateMsg = "Duplicate extension " + type.getName() + " name " + name + " on " + c.getName() + " and " + clazz.getName();
            logger.error(duplicateMsg);
            throw new IllegalStateException(duplicateMsg);
        }
    }

    /**
     * cache Activate class which is annotated with <code>Activate</code>
     * <p>
     * for compatibility, also cache class with old alibaba Activate annotation
     */
    private void cacheActivateClass(Class<?> clazz, String name) {
        // 在加载实现类的时候，如果说发现你的实现类加了@Activate注解
        // 这个实现类的@Activate注解，就会被放到cachedActivates缓存里面去
        Activate activate = clazz.getAnnotation(Activate.class);
        if (activate != null) {
            cachedActivates.put(name, activate);
        } else {
            // support com.alibaba.dubbo.common.extension.Activate
            com.alibaba.dubbo.common.extension.Activate oldActivate = clazz.getAnnotation(com.alibaba.dubbo.common.extension.Activate.class);
            if (oldActivate != null) {
                cachedActivates.put(name, oldActivate);
            }
        }
    }

    /**
     * cache Adaptive class which is annotated with <code>Adaptive</code>
     */
    private void cacheAdaptiveClass(Class<?> clazz, boolean overridden) {
        if (cachedAdaptiveClass == null || overridden) {
            cachedAdaptiveClass = clazz;
        } else if (!cachedAdaptiveClass.equals(clazz)) {
            throw new IllegalStateException("More than 1 adaptive class found: "
                + cachedAdaptiveClass.getName()
                + ", " + clazz.getName());
        }
    }

    /**
     * cache wrapper class
     * <p>
     * like: ProtocolFilterWrapper, ProtocolListenerWrapper
     */
    private void cacheWrapperClass(Class<?> clazz) {
        if (cachedWrapperClasses == null) {
            cachedWrapperClasses = new ConcurrentHashSet<>();
        }
        cachedWrapperClasses.add(clazz);
    }

    /**
     * test if clazz is a wrapper class
     * <p>
     * which has Constructor with given class type as its only argument
     */
    private boolean isWrapperClass(Class<?> clazz) {
        try {
            clazz.getConstructor(type);
            return true;
        } catch (NoSuchMethodException e) {
            return false;
        }
    }

    @SuppressWarnings("deprecation")
    private String findAnnotationName(Class<?> clazz) {
        org.apache.dubbo.common.Extension extension = clazz.getAnnotation(org.apache.dubbo.common.Extension.class);
        if (extension != null) {
            return extension.value();
        }

        String name = clazz.getSimpleName();
        if (name.endsWith(type.getSimpleName())) {
            name = name.substring(0, name.length() - type.getSimpleName().length());
        }
        return name.toLowerCase();
    }

    @SuppressWarnings("unchecked")
    private T createAdaptiveExtension() {
        try {
            // 如何去获取到一个符合自适应机制的这样的一个extension class
            T instance = (T) getAdaptiveExtensionClass().newInstance();
            instance = postProcessBeforeInitialization(instance, null);
            instance = injectExtension(instance);
            instance = postProcessAfterInitialization(instance, null);
            initExtension(instance);
            return instance;
        } catch (Exception e) {
            throw new IllegalStateException("Can't create adaptive extension " + type + ", cause: " + e.getMessage(), e);
        }
    }

    private Class<?> getAdaptiveExtensionClass() {
        // 在这个get extension classess过程中，会根据接口的配置文件扫描和加载所有的实现类
        // 对@Adaptive注解的处理，也会在这里进行
        getExtensionClasses();

        // @Adaptive注解的class，缓存好了以后，直接在这里做一个返回就可以了
        if (cachedAdaptiveClass != null) {
            return cachedAdaptiveClass;
        }
        // 光是加载extension classes还不够，还需要具体的创建adaptive自适应的扩展类
        return cachedAdaptiveClass = createAdaptiveExtensionClass();
    }

    private Class<?> createAdaptiveExtensionClass() {
        // Adaptive Classes' ClassLoader should be the same with Real SPI interface classes' ClassLoader
        ClassLoader classLoader = type.getClassLoader();
        try {
            if (NativeUtils.isNative()) {
                return classLoader.loadClass(type.getName() + "$Adaptive");
            }
        } catch (Throwable ignore) {

        }
        // Adaptive，他是用class code generator，做类代码生成
        // 他的类代码，是动态生成出来的
        String code = new AdaptiveClassCodeGenerator(type, cachedDefaultName).generate();
        // 在这里，还会对动态生成的类代码进行动态编译，编译Class字节码对象
        org.apache.dubbo.common.compiler.Compiler compiler = extensionDirector.getExtensionLoader(
            org.apache.dubbo.common.compiler.Compiler.class).getAdaptiveExtension();
        return compiler.compile(code, classLoader);
    }

    private Environment getEnvironment() {
        if (environment == null) {
            environment = (Environment) extensionDirector.getExtensionLoader(ApplicationExt.class).getExtension(Environment.NAME);
        }
        return environment;
    }

    @Override
    public String toString() {
        return this.getClass().getName() + "[" + type.getName() + "]";
    }

}
