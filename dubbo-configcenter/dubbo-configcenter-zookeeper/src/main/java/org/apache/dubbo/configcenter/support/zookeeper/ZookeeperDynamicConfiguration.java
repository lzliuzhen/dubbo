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
package org.apache.dubbo.configcenter.support.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigItem;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.TreePathDynamicConfiguration;
import org.apache.dubbo.common.threadpool.support.AbortPolicyWithReport;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NamedThreadFactory;
import org.apache.dubbo.remoting.zookeeper.ZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;
import org.apache.zookeeper.data.Stat;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * 作为一款成熟的框架，一个是自己要做的非常优秀，很好
 * 你的很多特性，是支持跟主流的一些外部的框架，或者是系统可以进行无缝对接
 * 这些事情，都是一个成熟的框架，必然要去做的
 *
 */
public class ZookeeperDynamicConfiguration extends TreePathDynamicConfiguration {

    private Executor executor;
    // The final root path would be: /configRootPath/"config"
    private String rootPath;
    private ZookeeperClient zkClient;

    private CacheListener cacheListener;
    private URL url;
    private static final int DEFAULT_ZK_EXECUTOR_THREADS_NUM = 1;
    private static final int DEFAULT_QUEUE = 10000;
    private static final Long THREAD_KEEP_ALIVE_TIME = 0L;

    ZookeeperDynamicConfiguration(URL url, ZookeeperTransporter zookeeperTransporter) {
        // url其实就是一个zk的连接地址
        super(url);

        this.url = url;
        rootPath = getRootPath(url);

        this.cacheListener = new CacheListener(rootPath);

        final String threadName = this.getClass().getSimpleName();

        // 构建一个他需要使用的线程池
        this.executor = new ThreadPoolExecutor(DEFAULT_ZK_EXECUTOR_THREADS_NUM, DEFAULT_ZK_EXECUTOR_THREADS_NUM,
                THREAD_KEEP_ALIVE_TIME, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(DEFAULT_QUEUE),
                new NamedThreadFactory(threadName, true),
                new AbortPolicyWithReport(threadName, url));

        // 基于ZooKeeperTransporter连接到了zk url去，拿到了一个ZookeeperClient
        zkClient = zookeeperTransporter.connect(url);
        boolean isConnected = zkClient.isConnected();
        if (!isConnected) {
            throw new IllegalStateException("Failed to connect with zookeeper, pls check if url " + url + " is correct.");
        }

        // 我们可以想一下，我们会怎么去用他，你可以把一些配置写入到zk里去，也可以通过zk读取一些配置
        // 类似于我们的mesh rule，他都用到了dynamic configuration，他会针对自己的router路由规则节点，施加监听
        // 如果有路由规则有变化，就会推送回来，自己这里就能感知到了
    }

    /**
     * @param key e.g., {service}.configurators, {service}.tagrouters, {group}.dubbo.properties
     * @return
     */
    @Override
    public String getInternalProperty(String key) {
        return zkClient.getContent(buildPathKey("", key));
    }

    @Override
    protected void doClose() throws Exception {
        // zkClient is shared in framework, should not close it here
        // zkClient.close();
        // See: org.apache.dubbo.remoting.zookeeper.AbstractZookeeperTransporter#destroy()
        // All zk clients is created and destroyed in ZookeeperTransporter.
        zkClient = null;
    }

    @Override
    protected boolean doPublishConfig(String pathKey, String content) throws Exception {
        // 就是说是你要把一个配置项写入到zk里去，配置项你要自定义一个path
        zkClient.create(pathKey, content, false);
        return true;
    }

    @Override
    public boolean publishConfigCas(String key, String group, String content, Object ticket) {
        try {
            if (ticket != null && !(ticket instanceof Stat)) {
                throw new IllegalArgumentException("zookeeper publishConfigCas requires stat type ticket");
            }
            String pathKey = buildPathKey(group, key);
            zkClient.createOrUpdate(pathKey, content, false, ticket == null ? 0 : ((Stat) ticket).getVersion());
            return true;
        } catch (Exception e) {
            logger.warn("zookeeper publishConfigCas failed.", e);
            return false;
        }
    }

    @Override
    protected String doGetConfig(String pathKey) throws Exception {
        // 从zk里获取一个配置项
        return zkClient.getContent(pathKey);
    }

    @Override
    public ConfigItem getConfigItem(String key, String group) {
        String pathKey = buildPathKey(group, key);
        return zkClient.getConfigItem(pathKey);
    }

    @Override
    protected boolean doRemoveConfig(String pathKey) throws Exception {
        zkClient.delete(pathKey);
        return true;
    }

    @Override
    protected Collection<String> doGetConfigKeys(String groupPath) {
        return zkClient.getChildren(groupPath);
    }

    @Override
    protected void doAddListener(String pathKey, ConfigurationListener listener) {
        // 针对指定的配置项，去加一个监听器
        cacheListener.addListener(pathKey, listener);
        zkClient.addDataListener(pathKey, cacheListener, executor);
    }

    @Override
    protected void doRemoveListener(String pathKey, ConfigurationListener listener) {
        cacheListener.removeListener(pathKey, listener);
        Set<ConfigurationListener> configurationListeners = cacheListener.getConfigurationListeners(pathKey);
        if (CollectionUtils.isNotEmpty(configurationListeners)) {
            zkClient.removeDataListener(pathKey, cacheListener);
        }
    }
}
