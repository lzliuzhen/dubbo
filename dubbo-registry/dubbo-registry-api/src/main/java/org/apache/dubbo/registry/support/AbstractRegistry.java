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
package org.apache.dubbo.registry.support;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.common.utils.ConfigUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.Registry;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.dubbo.common.constants.CommonConstants.ANY_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.COMMA_SPLIT_PATTERN;
import static org.apache.dubbo.common.constants.CommonConstants.FILE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.REGISTRY_LOCAL_FILE_CACHE_ENABLED;
import static org.apache.dubbo.common.constants.RegistryConstants.ACCEPTS_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;
import static org.apache.dubbo.common.constants.RegistryConstants.DYNAMIC_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.registry.Constants.CACHE;
import static org.apache.dubbo.registry.Constants.DUBBO_REGISTRY;
import static org.apache.dubbo.registry.Constants.REGISTRY_FILESAVE_SYNC_KEY;
import static org.apache.dubbo.registry.Constants.USER_HOME;

/**
 * AbstractRegistry. (SPI, Prototype, ThreadSafe)
 *
 * 主要应该是包含了注册中心里面一些通用的、公共的能力
 * 两大块公共基础能力，基于本地磁盘文件的数据缓存写入和重启恢复加载的一套机制
 * 注册、取消注册、订阅、取消订阅、变更通知，注册中心相关的交互逻辑，在这里都有对应的数据结构存储
 *
 */
public abstract class AbstractRegistry implements Registry {

    // URL address separator, used in file cache, service provider URL separation
    private static final char URL_SEPARATOR = ' ';
    // URL address separated regular expression for parsing the service provider URL list in the file cache
    private static final String URL_SPLIT = "\\s+";
    // Max times to retry to save properties to local cache file
    private static final int MAX_RETRY_TIMES_SAVE_PROPERTIES = 3;
    // Log output
    protected final Logger logger = LoggerFactory.getLogger(getClass());
    // Local disk cache, where the special key value.registries records the list of registry centers, and the others are the list of notified service providers
    // local磁盘缓存，存储了一些缓存的数据
    private final Properties properties = new Properties();
    // File cache timing writing
    // 包含了注册缓存数据的线程池
    private final ExecutorService registryCacheExecutor;
    // 最近一次缓存变更时间戳，long，timestamp
    private final AtomicLong lastCacheChanged = new AtomicLong();
    // 存储properties重试次数
    private final AtomicInteger savePropertiesRetryTimes = new AtomicInteger();
    // 已经注册过的url地址的存储，就放在set集合里
    private final Set<URL> registered = new ConcurrentHashSet<>();
    // 针对url地址执行的订阅，consumer里面，你对一个url地址执行了订阅，你的监听器就会被放在这里
    private final ConcurrentMap<URL, Set<NotifyListener>> subscribed = new ConcurrentHashMap<>();
    // 如果说订阅之后，有一些服务实例的变更，zk会反向通知，此时会出现notify操作，url地址如果有反向推送变更通知，此时会在这里
    private final ConcurrentMap<URL, Map<String, List<URL>>> notified = new ConcurrentHashMap<>();
    // Is it synchronized to save the file
    // 是否要同步存储数据到磁盘文件里去
    private boolean syncSaveFile;
    // 注册中心的url地址，比如说zk，zookeeper://ip:2181/
    private URL registryUrl;
    // Local disk cache file
    // 本地磁盘上面的缓存文件的File句柄
    private File file;
    // 本地缓存是否启用
    private boolean localCacheEnabled;
    // 注册中心管理组件，manager
    private RegistryManager registryManager;

    public AbstractRegistry(URL url) {
        setUrl(url); // 设置注册中心的url地址

        // 从model组建里获取bean工厂，从bean容器里获取RegistryManager实例
        registryManager = url.getOrDefaultApplicationModel().getBeanFactory().getBean(RegistryManager.class);
        // 本地磁盘缓存是否开启，默认就是开启的，你的每个provider或者consumer，但凡用了zookeeper registry，人家都会自动开启本地磁盘缓存
        // 这样的话就是说可以确保，你要是自己突然宕机了，重启，重新注册和订阅，但是在这个过程中，磁盘里缓存的一些数据，可以从磁盘里恢复出来，继续使用
        localCacheEnabled = url.getParameter(REGISTRY_LOCAL_FILE_CACHE_ENABLED, true);
        // 获取到一个共享线程池，cache线程池，线程数量无限大，但是空闲超过60s的线程会自动回收
        // 通过model组件拿到SPI，SPI去获取组件接口的实例，在任何地方，都可以拿到公共使用的线程池存储组件，从里面拿到自己需要的线程池
        registryCacheExecutor = url.getOrDefaultApplicationModel().getDefaultExtension(ExecutorRepository.class).getSharedExecutor();

        // 如果本地缓存启用了，就会去进行磁盘持久化
        if (localCacheEnabled) {
            // Start file save timer
            // 启动一个自动刷盘存储的定时器，timer，一直不停的执行
            // 肯定是内存里的数据会不停的变化，会有一个定时器，定时的把数据写入到磁盘里去
            // 本地磁盘的缓存，如果说你突然崩溃了，重启之后要重新注册，或者是订阅发现
            // 但是此时，万一说，zk此时不可用，服务订阅和发现，短时间内就没法去做
            // 此时可以从磁盘缓存里恢复一些数据回来，之前订阅和发现过的数据，都是可以使用的

            // 是否同步写入磁盘文件里去，默认是false
            syncSaveFile = url.getParameter(REGISTRY_FILESAVE_SYNC_KEY, false);

            // 默认的磁盘文件的目录和文件名，user home（操作系统他的用户的目录）
            // /zhangsan/.dubbo/dubbo-registry-demo-zookeeper-127.0.0.1-2181.cache
            String defaultFilename = System.getProperty(USER_HOME) + DUBBO_REGISTRY +
                url.getApplication() + "-" + url.getAddress().replaceAll(":", "-") + CACHE;
            String filename = url.getParameter(FILE_KEY, defaultFilename);
            // 把文件名称封装为File对象
            File file = null;
            if (ConfigUtils.isNotEmpty(filename)) {
                file = new File(filename);
                // 是属于对文件的上级目录的处理
                if (!file.exists() && file.getParentFile() != null && !file.getParentFile().exists()) {
                    if (!file.getParentFile().mkdirs()) {
                        throw new IllegalArgumentException("Invalid registry cache file " + file + ", cause: Failed to create directory " + file.getParentFile() + "!");
                    }
                }
            }
            this.file = file;

            // When starting the subscription center,
            // we need to read the local cache file for future Registry fault tolerance processing.
            // 假设说是第一次启动一个干净的服务，此时本地缓存里是什么都没有的，是空的
            // 但是如果是之前启动过，订阅和发现过服务数据，所以说本地磁盘里都是有缓存数据的，再次重启就可以先加载本地磁盘里的缓存数据
            // 就是说把本地磁盘里的数据，都加载到properties里去，做好一个准备，万一注册中心此时故障，就可以用缓存数据了
            loadProperties();
            // notify概念，从我们的注册中心的url地址里，获取backup备用一批url地址
            // 拿到这批备用url地址之后，做了一个notify操作
            notify(url.getBackupUrls());
        }
    }

    protected static List<URL> filterEmpty(URL url, List<URL> urls) {
        if (CollectionUtils.isEmpty(urls)) {
            List<URL> result = new ArrayList<>(1);
            result.add(url.setProtocol(EMPTY_PROTOCOL));
            return result;
        }
        return urls;
    }

    @Override
    public URL getUrl() {
        return registryUrl;
    }

    protected void setUrl(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("registry url == null");
        }
        this.registryUrl = url;
    }

    public Set<URL> getRegistered() {
        return Collections.unmodifiableSet(registered);
    }

    public Map<URL, Set<NotifyListener>> getSubscribed() {
        return Collections.unmodifiableMap(subscribed);
    }

    public Map<URL, Map<String, List<URL>>> getNotified() {
        return Collections.unmodifiableMap(notified);
    }

    public File getCacheFile() {
        return file;
    }

    public Properties getCacheProperties() {
        return properties;
    }

    public AtomicLong getLastCacheChanged() {
        return lastCacheChanged;
    }

    public void doSaveProperties(long version) {
        if (version < lastCacheChanged.get()) {
            return;
        }
        if (file == null) {
            return;
        }
        // Save
        try {
            // dubbo写的一些磁盘io操作，都是很经典的，值得我们去看一下的
            // 如果要对本地磁盘发起一些io操作，一般来说都是要建立一个磁盘文件锁的
            File lockfile = new File(file.getAbsolutePath() + ".lock");
            if (!lockfile.exists()) {
                lockfile.createNewFile();
            }
            // 针对锁文件，lock file，搞一个RandomAccessFile
            try (RandomAccessFile raf = new RandomAccessFile(lockfile, "rw");
                FileChannel channel = raf.getChannel()) {
                // 此时就可以在API代码层面，针对这个锁文件进行lock加锁
                // 此时如果有其他的线程，代码也走到了这里，就会直接被block住，锁只能是一个人去加
                FileLock lock = channel.tryLock();
                if (lock == null) {
                    throw new IOException("Can not lock the registry cache file " + file.getAbsolutePath() + ", ignore and retry later, maybe multi java process use the file, please config: dubbo.registry.file=xxx.properties");
                }
                // Save
                try {
                    if (!file.exists()) {
                        file.createNewFile();
                    }
                    // 磁盘io，file output stream
                    try (FileOutputStream outputFile = new FileOutputStream(file)) {
                        // 直接基于jdk提供的API，进行文件io操作
                        properties.store(outputFile, "Dubbo Registry Cache");
                    }
                } finally {
                    lock.release();
                }
            }
        } catch (Throwable e) {
            // 万一说刷盘的时候要是失败了，此时还可以执行重试刷盘的策略
            savePropertiesRetryTimes.incrementAndGet();
            if (savePropertiesRetryTimes.get() >= MAX_RETRY_TIMES_SAVE_PROPERTIES) {
                logger.warn("Failed to save registry cache file after retrying " + MAX_RETRY_TIMES_SAVE_PROPERTIES + " times, cause: " + e.getMessage(), e);
                savePropertiesRetryTimes.set(0);
                return;
            }
            if (version < lastCacheChanged.get()) {
                savePropertiesRetryTimes.set(0);
                return;
            } else {
                registryCacheExecutor.execute(new SaveProperties(lastCacheChanged.incrementAndGet()));
            }
            logger.warn("Failed to save registry cache file, will retry, cause: " + e.getMessage(), e);
        }
    }

    // 如果一旦说启用了这个缓存机制，服务发现的数据，存储到本地磁盘去
    // 服务实例后续如果说重启的话，这里就会从本地磁盘拿到你需要的缓存数据就可以了
    private void loadProperties() {
        if (file != null && file.exists()) {
            InputStream in = null;
            try {
                // 直接搞一个file input stream
                in = new FileInputStream(file);
                properties.load(in);
                if (logger.isInfoEnabled()) {
                    logger.info("Loaded registry cache file " + file);
                }
            } catch (Throwable e) {
                logger.warn("Failed to load registry cache file " + file, e);
            } finally {
                if (in != null) {
                    try {
                        in.close();
                    } catch (IOException e) {
                        logger.warn(e.getMessage(), e);
                    }
                }
            }
        }
    }

    public List<URL> getCacheUrls(URL url) {
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (StringUtils.isNotEmpty(key) && key.equals(url.getServiceKey())
                && (Character.isLetter(key.charAt(0)) || key.charAt(0) == '_')
                && StringUtils.isNotEmpty(value)) {
                String[] arr = value.trim().split(URL_SPLIT);
                List<URL> urls = new ArrayList<>();
                for (String u : arr) {
                    urls.add(URL.valueOf(u));
                }
                return urls;
            }
        }
        return null;
    }

    @Override
    public List<URL> lookup(URL url) {
        List<URL> result = new ArrayList<>();
        Map<String, List<URL>> notifiedUrls = getNotified().get(url);
        if (CollectionUtils.isNotEmptyMap(notifiedUrls)) {
            for (List<URL> urls : notifiedUrls.values()) {
                for (URL u : urls) {
                    if (!EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        } else {
            final AtomicReference<List<URL>> reference = new AtomicReference<>();
            NotifyListener listener = reference::set;
            subscribe(url, listener); // Subscribe logic guarantees the first notify to return
            List<URL> urls = reference.get();
            if (CollectionUtils.isNotEmpty(urls)) {
                for (URL u : urls) {
                    if (!EMPTY_PROTOCOL.equals(u.getProtocol())) {
                        result.add(u);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public void register(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("register url == null");
        }
        if (url.getPort() != 0) {
            if (logger.isInfoEnabled()) {
                logger.info("Register: " + url);
            }
        }
        // 在这里就是维护一个公共数据结构，已经注册过的url这里添加到一个数据结构里去
        registered.add(url);
    }

    @Override
    public void unregister(URL url) {
        if (url == null) {
            throw new IllegalArgumentException("unregister url == null");
        }
        if (url.getPort() != 0) {
            if (logger.isInfoEnabled()) {
                logger.info("Unregister: " + url);
            }
        }
        registered.remove(url);
    }

    @Override
    public void subscribe(URL url, NotifyListener listener) {
        if (url == null) {
            throw new IllegalArgumentException("subscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("subscribe listener == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Subscribe: " + url);
        }

        // 针对我们当前要订阅的这个url地址，应该来说就是我们要关注的那个服务接口
        // 针对一个服务的订阅监听，可能会施加多个监听器
        // 此时会把监听器，加入到这个set里面去
        // 订阅和监听的时候，必须是要加一个监听器的，NotifyListener，这个并不是说我们的zk里面的监听器
        // child listener，watcher
        Set<NotifyListener> listeners = subscribed.computeIfAbsent(url, n -> new ConcurrentHashSet<>());
        listeners.add(listener);
    }

    @Override
    public void unsubscribe(URL url, NotifyListener listener) {
        if (url == null) {
            throw new IllegalArgumentException("unsubscribe url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("unsubscribe listener == null");
        }
        if (logger.isInfoEnabled()) {
            logger.info("Unsubscribe: " + url);
        }
        Set<NotifyListener> listeners = subscribed.get(url);
        if (listeners != null) {
            listeners.remove(listener);
        }

        // do not forget remove notified
        notified.remove(url);
    }

    protected void recover() throws Exception {
        // register
        Set<URL> recoverRegistered = new HashSet<>(getRegistered());
        if (!recoverRegistered.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover register url " + recoverRegistered);
            }
            for (URL url : recoverRegistered) {
                register(url);
            }
        }
        // subscribe
        Map<URL, Set<NotifyListener>> recoverSubscribed = new HashMap<>(getSubscribed());
        if (!recoverSubscribed.isEmpty()) {
            if (logger.isInfoEnabled()) {
                logger.info("Recover subscribe url " + recoverSubscribed.keySet());
            }
            for (Map.Entry<URL, Set<NotifyListener>> entry : recoverSubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    subscribe(url, listener);
                }
            }
        }
    }

    protected void notify(List<URL> urls) {
        if (CollectionUtils.isEmpty(urls)) {
            return;
        }

        // 刚刚开始，他的subscribed订阅数据里应该是空的，for循环根本就不会执行了
        for (Map.Entry<URL, Set<NotifyListener>> entry : getSubscribed().entrySet()) {
            URL url = entry.getKey();

            if (!UrlUtils.isMatch(url, urls.get(0))) {
                continue;
            }

            Set<NotifyListener> listeners = entry.getValue();
            if (listeners != null) {
                for (NotifyListener listener : listeners) {
                    try {
                        notify(url, listener, filterEmpty(url, urls));
                    } catch (Throwable t) {
                        logger.error("Failed to notify registry event, urls: " + urls + ", cause: " + t.getMessage(), t);
                    }
                }
            }
        }
    }

    /**
     * Notify changes from the Provider side.
     *
     * @param url      consumer side url
     * @param listener listener
     * @param urls     provider latest urls
     */
    protected void notify(URL url, NotifyListener listener, List<URL> urls) {
        if (url == null) {
            throw new IllegalArgumentException("notify url == null");
        }
        if (listener == null) {
            throw new IllegalArgumentException("notify listener == null");
        }
        if ((CollectionUtils.isEmpty(urls))
            && !ANY_VALUE.equals(url.getServiceInterface())) {
            logger.warn("Ignore empty notify urls for subscribe url " + url);
            return;
        }
        if (logger.isInfoEnabled()) {
            logger.info("Notify urls for subscribe url " + url + ", url size: " + urls.size());
        }
        // keep every provider's category.
        // map，key正常来说就是/dubbo/服务接口/providers，urls list
        Map<String, List<URL>> result = new HashMap<>();
        for (URL u : urls) {
            if (UrlUtils.isMatch(url, u)) {
                String category = u.getCategory(DEFAULT_CATEGORY);
                List<URL> categoryList = result.computeIfAbsent(category, k -> new ArrayList<>());
                categoryList.add(u);
            }
        }
        if (result.size() == 0) {
            return;
        }
        Map<String, List<URL>> categoryNotified = notified.computeIfAbsent(url, u -> new ConcurrentHashMap<>());
        for (Map.Entry<String, List<URL>> entry : result.entrySet()) {
            String category = entry.getKey();
            List<URL> categoryList = entry.getValue();
            categoryNotified.put(category, categoryList);
            listener.notify(categoryList);
            // We will update our cache file after each notification.
            // When our Registry has a subscribe failure due to network jitter, we can return at least the existing cache URL.
            if (localCacheEnabled) {
                saveProperties(url);
            }
        }
    }

    private void saveProperties(URL url) {
        if (file == null) {
            return;
        }

        try {
            StringBuilder buf = new StringBuilder();
            Map<String, List<URL>> categoryNotified = notified.get(url);
            if (categoryNotified != null) {
                for (List<URL> us : categoryNotified.values()) {
                    for (URL u : us) {
                        if (buf.length() > 0) {
                            buf.append(URL_SEPARATOR);
                        }
                        buf.append(u.toFullString());
                    }
                }
            }
            properties.setProperty(url.getServiceKey(), buf.toString());
            long version = lastCacheChanged.incrementAndGet();
            if (syncSaveFile) {
                doSaveProperties(version);
            } else {
                registryCacheExecutor.execute(new SaveProperties(version));
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage(), t);
        }
    }

    @Override
    public void destroy() {
        if (logger.isInfoEnabled()) {
            logger.info("Destroy registry:" + getUrl());
        }
        Set<URL> destroyRegistered = new HashSet<>(getRegistered());
        if (!destroyRegistered.isEmpty()) {
            for (URL url : new HashSet<>(destroyRegistered)) {
                if (url.getParameter(DYNAMIC_KEY, true)) {
                    try {
                        unregister(url);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unregister url " + url);
                        }
                    } catch (Throwable t) {
                        logger.warn("Failed to unregister url " + url + " to registry " + getUrl() + " on destroy, cause: " + t.getMessage(), t);
                    }
                }
            }
        }
        Map<URL, Set<NotifyListener>> destroySubscribed = new HashMap<>(getSubscribed());
        if (!destroySubscribed.isEmpty()) {
            for (Map.Entry<URL, Set<NotifyListener>> entry : destroySubscribed.entrySet()) {
                URL url = entry.getKey();
                for (NotifyListener listener : entry.getValue()) {
                    try {
                        unsubscribe(url, listener);
                        if (logger.isInfoEnabled()) {
                            logger.info("Destroy unsubscribe url " + url);
                        }
                    } catch (Throwable t) {
                        logger.warn("Failed to unsubscribe url " + url + " to registry " + getUrl() + " on destroy, cause: " + t.getMessage(), t);
                    }
                }
            }
        }
        registryManager.removeDestroyedRegistry(this);
    }

    protected boolean acceptable(URL urlToRegistry) {
        String pattern = registryUrl.getParameter(ACCEPTS_KEY);
        if (StringUtils.isEmpty(pattern)) {
            return true;
        }

        return Arrays.stream(COMMA_SPLIT_PATTERN.split(pattern))
            .anyMatch(p -> p.equalsIgnoreCase(urlToRegistry.getProtocol()));
    }

    @Override
    public String toString() {
        return getUrl().toString();
    }

    private class SaveProperties implements Runnable {
        private long version;

        private SaveProperties(long version) {
            this.version = version;
        }

        @Override
        public void run() {
            doSaveProperties(version);
        }
    }

}
