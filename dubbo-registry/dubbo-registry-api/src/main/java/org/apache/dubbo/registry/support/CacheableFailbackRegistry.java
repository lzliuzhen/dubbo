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
import org.apache.dubbo.common.URLBuilder;
import org.apache.dubbo.common.URLStrParser;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.url.component.DubboServiceAddressURL;
import org.apache.dubbo.common.url.component.ServiceAddressURL;
import org.apache.dubbo.common.url.component.URLAddress;
import org.apache.dubbo.common.url.component.URLParam;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.ProviderFirstParams;
import org.apache.dubbo.rpc.model.ScopeModel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.URLStrParser.ENCODED_AND_MARK;
import static org.apache.dubbo.common.URLStrParser.ENCODED_PID_KEY;
import static org.apache.dubbo.common.URLStrParser.ENCODED_QUESTION_MARK;
import static org.apache.dubbo.common.URLStrParser.ENCODED_TIMESTAMP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.CACHE_CLEAR_TASK_INTERVAL;
import static org.apache.dubbo.common.constants.CommonConstants.CACHE_CLEAR_WAITING_THRESHOLD;
import static org.apache.dubbo.common.constants.CommonConstants.CHECK_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.DUBBO;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_SEPARATOR;
import static org.apache.dubbo.common.constants.CommonConstants.PROTOCOL_SEPARATOR_ENCODED;
import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.EMPTY_PROTOCOL;
import static org.apache.dubbo.common.constants.RegistryConstants.PROVIDERS_CATEGORY;

/**
 * Useful for registries who's sdk returns raw string as provider instance, for example, zookeeper and etcd.
 *
 * zk里面，每个provider服务实例的信息都封装在一个url里面（字符串，dubbo://ip:port/xx=xx&xx=xx）
 * 从zk里面获取到provider地址的时候，url字符串的方式来获取的，此时是很有用的
 *
 * Failback，集群容错那个模块里看到过，FailbackClusterInvoker
 * 一旦有一些失败和故障，此时会对失败和故障信息做一个存储，隔一段时间再次重新去进行重试
 * Cacheable，可缓存的，注册中心是支持一些缓存机制在里面的，服务订阅和发现，必须要支持缓存机制，能把发现的东西缓存起来
 *
 * 看起来这个类主要是用于实现一些缓存机制的
 *
 * 子类叫这个名字，建立在父类的failback故障重试基础之上的缓存机制
 *
 */
public abstract class CacheableFailbackRegistry extends FailbackRegistry {
    private static final Logger logger = LoggerFactory.getLogger(CacheableFailbackRegistry.class);
    private static String[] VARIABLE_KEYS = new String[]{ENCODED_TIMESTAMP_KEY, ENCODED_PID_KEY};

    protected Map<String, URLAddress> stringAddress = new ConcurrentHashMap<>();
    protected Map<String, URLParam> stringParam = new ConcurrentHashMap<>();
    private ScheduledExecutorService cacheRemovalScheduler;
    private int cacheRemovalTaskIntervalInMillis;
    private int cacheClearWaitingThresholdInMillis;
    private Map<ServiceAddressURL, Long> waitForRemove = new ConcurrentHashMap<>();
    private Semaphore semaphore = new Semaphore(1);

    private final Map<String, String> extraParameters;
    // 这里就是一些缓存机制会在这个里面
    protected final Map<URL, Map<String, ServiceAddressURL>> stringUrls = new HashMap<>();

    public CacheableFailbackRegistry(URL url) {
        // 继续往父类构造函数去走
        super(url);

        // 构建extraParameters存储map
        extraParameters = new HashMap<>(8);
        extraParameters.put(CHECK_KEY, String.valueOf(false));
        // 缓存定时清理任务，此时也是用单线程的线程池就可以了
        cacheRemovalScheduler = url.getOrDefaultApplicationModel().getExtensionLoader(ExecutorRepository.class).getDefaultExtension().nextScheduledExecutor();
        // 缓存定时清理任务，时间间隔，默认是2分钟
        cacheRemovalTaskIntervalInMillis = getIntConfig(url.getScopeModel(), CACHE_CLEAR_TASK_INTERVAL, 2 * 60 * 1000);
        // 缓存清理等待阈值，时间，5分钟
        cacheClearWaitingThresholdInMillis = getIntConfig(url.getScopeModel(), CACHE_CLEAR_WAITING_THRESHOLD, 5 * 60 * 1000);
    }

    protected static int getIntConfig(ScopeModel scopeModel, String key, int def) {
        String str = ConfigurationUtils.getProperty(scopeModel, key);
        int result = def;
        if (StringUtils.isNotEmpty(str)) {
            try {
                result = Integer.parseInt(str);
            } catch (NumberFormatException e) {
                logger.warn("Invalid registry properties configuration key " + key + ", value " + str);
            }
        }
        return result;
    }

    @Override
    public void doUnsubscribe(URL url, NotifyListener listener) {
        this.evictURLCache(url);
    }

    protected void evictURLCache(URL url) {
        // 对于此时的url，remove出来的是一个老缓存
        Map<String, ServiceAddressURL> oldURLs = stringUrls.remove(url);
        try {
            if (oldURLs != null && oldURLs.size() > 0) {
                logger.info("Evicting urls for service " + url.getServiceKey() + ", size " + oldURLs.size());
                Long currentTimestamp = System.currentTimeMillis();
                // 放你要待清理的URL对象，跟当前时间戳，放到一个等待清理的数据结构里去
                for (Map.Entry<String, ServiceAddressURL> entry : oldURLs.entrySet()) {
                    waitForRemove.put(entry.getValue(), currentTimestamp);
                }
                if (CollectionUtils.isNotEmptyMap(waitForRemove)) {
                    if (semaphore.tryAcquire()) {
                        cacheRemovalScheduler.schedule(new RemovalTask(), cacheRemovalTaskIntervalInMillis, TimeUnit.MILLISECONDS);
                    }
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to evict url for " + url.getServiceKey(), e);
        }
    }

    // toUrlsWithoutEmpty，如果大家还有点印象的话，就会记得，在我们的ZooKeeperRegistry里
    // 我们其实是有这个方法的调用的，把服务发现拿到的一批raw string格式的url
    // 需要把他们都转换为一个URL list
    protected List<URL> toUrlsWithoutEmpty(URL consumer, Collection<String> providers) {
        // keep old urls
        Map<String, ServiceAddressURL> oldURLs = stringUrls.get(consumer);
        // create new urls
        Map<String, ServiceAddressURL> newURLs;
        URL copyOfConsumer = removeParamsFromConsumer(consumer);
        if (oldURLs == null) {
            newURLs = new HashMap<>();
            for (String rawProvider : providers) {
                rawProvider = stripOffVariableKeys(rawProvider);
                ServiceAddressURL cachedURL = createURL(rawProvider, copyOfConsumer, getExtraParameters());
                if (cachedURL == null) {
                    logger.warn("Invalid address, failed to parse into URL " + rawProvider);
                    continue;
                }
                newURLs.put(rawProvider, cachedURL);
            }
        } else {
            newURLs = new HashMap<>((int) (oldURLs.size() / .75 + 1));
            // maybe only default , or "env" + default
            for (String rawProvider : providers) {
                rawProvider = stripOffVariableKeys(rawProvider);
                // 如果说有一个变化，providers他的地址有变化，重新在进行raw url -> URL之间做一个处理的时候
                ServiceAddressURL cachedURL = oldURLs.remove(rawProvider);
                if (cachedURL == null) {
                    cachedURL = createURL(rawProvider, copyOfConsumer, getExtraParameters());
                    if (cachedURL == null) {
                        logger.warn("Invalid address, failed to parse into URL " + rawProvider);
                        continue;
                    }
                }
                // 如果对一个raw url之前已经创建过一个URL了，此时就不用重新创建了
                // 直接复用就可以了，这个就是所谓的经典的缓存机制
                // 把你的raw url -> URL之间做一个缓存，下次有一些变动要重新计算的时候，此时直接用缓存就可以了
                // 但是如果有一些新的provider raw url，此时就不能够用缓存了，就需要重新创建一个URL
                newURLs.put(rawProvider, cachedURL);
            }
        }

        // 当你的consumer对你的同样的服务的providers重复进行订阅和发现的时候
        // 已经有的raw url->URL，是直接用缓存的
        // 只有新的raw url才会搞一个新的URL对象，去使用

        // 每次处理完一批url之后，老的url可以复用，新的url是新创建的
        // 此时对这个consumer url -> map，先要去进行url缓存，老缓存清理
        evictURLCache(consumer);
        // 清理完了老缓存之后，再去放置新的缓存数据
        stringUrls.put(consumer, newURLs);

        return new ArrayList<>(newURLs.values());
    }

    // 他这个方法就是正儿八经的，在我们的ZooKeeper里进行服务订阅和发现之后，会调用的一个方法
    // 他是属于一个重要的入口，可以在整个订阅发现链路里，执行到这里来
    protected List<URL> toUrlsWithEmpty(URL consumer, String path, Collection<String> providers) {
        List<URL> urls = new ArrayList<>(1);
        boolean isProviderPath = path.endsWith(PROVIDERS_CATEGORY);
        if (isProviderPath) {
            if (CollectionUtils.isNotEmpty(providers)) {
                urls = toUrlsWithoutEmpty(consumer, providers);
            } else {
                // clear cache on empty notification: unsubscribe or provider offline
                evictURLCache(consumer);
            }
        } else {
            if (CollectionUtils.isNotEmpty(providers)) {
                urls = toConfiguratorsWithoutEmpty(consumer, providers);
            }
        }

        if (urls.isEmpty()) {
            int i = path.lastIndexOf(PATH_SEPARATOR);
            String category = i < 0 ? path : path.substring(i + 1);
            URL empty = URLBuilder.from(consumer)
                    .setProtocol(EMPTY_PROTOCOL)
                    .addParameter(CATEGORY_KEY, category)
                    .build();
            urls.add(empty);
        }

        return urls;
    }

    protected ServiceAddressURL createURL(String rawProvider, URL consumerURL, Map<String, String> extraParameters) {
        boolean encoded = true;
        // use encoded value directly to avoid URLDecoder.decode allocation.
        int paramStartIdx = rawProvider.indexOf(ENCODED_QUESTION_MARK);
        if (paramStartIdx == -1) {// if ENCODED_QUESTION_MARK does not show, mark as not encoded.
            encoded = false;
        }
        String[] parts = URLStrParser.parseRawURLToArrays(rawProvider, paramStartIdx);
        if (parts.length <= 1) {
            logger.warn("Received url without any parameters " + rawProvider);
            return DubboServiceAddressURL.valueOf(rawProvider, consumerURL);
        }

        String rawAddress = parts[0];
        String rawParams = parts[1];
        boolean isEncoded = encoded;

        // 每次在创建URL的时候，URLAddress和URLParam，如果能复用就可以直接复用就好了
        // 如果说没有的话，才去创建新的
        URLAddress address = stringAddress.computeIfAbsent(rawAddress, k -> URLAddress.parse(k, getDefaultURLProtocol(), isEncoded));
        address.setTimestamp(System.currentTimeMillis());

        URLParam param = stringParam.computeIfAbsent(rawParams, k -> URLParam.parse(k, isEncoded, extraParameters));
        param.setTimestamp(System.currentTimeMillis());

        ServiceAddressURL cachedURL = createServiceURL(address, param, consumerURL);
        if (isMatch(consumerURL, cachedURL)) {
            return cachedURL;
        }
        return null;
    }


    protected ServiceAddressURL createServiceURL(URLAddress address, URLParam param, URL consumerURL) {
        return new DubboServiceAddressURL(address, param, consumerURL, null);
    }

    protected URL removeParamsFromConsumer(URL consumer) {
        Set<ProviderFirstParams> providerFirstParams = consumer.getOrDefaultApplicationModel().getExtensionLoader(ProviderFirstParams.class).getSupportedExtensionInstances();
        if (CollectionUtils.isEmpty(providerFirstParams)) {
            return consumer;
        }

        for (ProviderFirstParams paramsFilter : providerFirstParams) {
            consumer = consumer.removeParameters(paramsFilter.params());
        }
        return consumer;
    }

    private String stripOffVariableKeys(String rawProvider) {
        String[] keys = getVariableKeys();
        if (keys == null || keys.length == 0) {
            return rawProvider;
        }

        for (String key : keys) {
            int idxStart = rawProvider.indexOf(key);
            if (idxStart == -1) {
                continue;
            }
            int idxEnd = rawProvider.indexOf(ENCODED_AND_MARK, idxStart);
            String part1 = rawProvider.substring(0, idxStart);
            if (idxEnd == -1) {
                rawProvider = part1;
            } else {
                String part2 = rawProvider.substring(idxEnd + ENCODED_AND_MARK.length());
                rawProvider = part1 + part2;
            }
        }

        if (rawProvider.endsWith(ENCODED_AND_MARK)) {
            rawProvider = rawProvider.substring(0, rawProvider.length() - ENCODED_AND_MARK.length());
        }
        if (rawProvider.endsWith(ENCODED_QUESTION_MARK)) {
            rawProvider = rawProvider.substring(0, rawProvider.length() - ENCODED_QUESTION_MARK.length());
        }

        return rawProvider;
    }

    private List<URL> toConfiguratorsWithoutEmpty(URL consumer, Collection<String> configurators) {
        List<URL> urls = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(configurators)) {
            for (String provider : configurators) {
                if (provider.contains(PROTOCOL_SEPARATOR_ENCODED)) {
                    URL url = URLStrParser.parseEncodedStr(provider);
                    if (UrlUtils.isMatch(consumer, url)) {
                        urls.add(url);
                    }
                }
            }
        }
        return urls;
    }

    protected Map<String, String> getExtraParameters() {
        return extraParameters;
    }

    protected String[] getVariableKeys() {
        return VARIABLE_KEYS;
    }

    protected String getDefaultURLProtocol() {
        return DUBBO;
    }

    /**
     * This method is for unit test to see if the RemovalTask has completed or not.<br />
     * <strong>Please do not call this method in other places.</strong>
     */
    @Deprecated
    protected Semaphore getSemaphore() {
        return semaphore;
    }

    protected abstract boolean isMatch(URL subscribeUrl, URL providerUrl);


    private class RemovalTask implements Runnable {
        @Override
        public void run() {
            logger.info("Clearing cached URLs, waiting to clear size " + waitForRemove.size());
            int clearCount = 0;
            try {
                Iterator<Map.Entry<ServiceAddressURL, Long>> it = waitForRemove.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<ServiceAddressURL, Long> entry = it.next();
                    ServiceAddressURL removeURL = entry.getKey();
                    long removeTime = entry.getValue();
                    long current = System.currentTimeMillis();
                    if (current - removeTime >= cacheClearWaitingThresholdInMillis) {
                        URLAddress urlAddress = removeURL.getUrlAddress();
                        URLParam urlParam = removeURL.getUrlParam();
                        if (current - urlAddress.getTimestamp() >= cacheClearWaitingThresholdInMillis) {
                            stringAddress.remove(urlAddress.getRawAddress());
                        }
                        if (current - urlParam.getTimestamp() >= cacheClearWaitingThresholdInMillis) {
                            stringParam.remove(urlParam.getRawParam());
                        }
                        it.remove();
                        clearCount++;
                    }
                }
            } catch (Throwable t) {
                logger.error("Error occurred when clearing cached URLs", t);
            } finally {
                semaphore.release();
            }
            logger.info("Clear cached URLs, size " + clearCount);

            // 等待删除的这个map里面的数据还没删干净，有些缓存数据没到5分钟
            // 此时就需要再次提交一个缓存清理任务
            if (CollectionUtils.isNotEmptyMap(waitForRemove)) {
                // move to next schedule
                if (semaphore.tryAcquire()) {
                    cacheRemovalScheduler.schedule(new RemovalTask(), cacheRemovalTaskIntervalInMillis, TimeUnit.MILLISECONDS);
                }
            }
        }
    }
}
