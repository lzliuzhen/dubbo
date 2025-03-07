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
package org.apache.dubbo.remoting.zookeeper.curator5;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigItem;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.remoting.zookeeper.AbstractZookeeperClient;
import org.apache.dubbo.remoting.zookeeper.ChildListener;
import org.apache.dubbo.remoting.zookeeper.DataListener;
import org.apache.dubbo.remoting.zookeeper.EventType;
import org.apache.dubbo.remoting.zookeeper.StateListener;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_KEY;


public class Curator5ZookeeperClient extends AbstractZookeeperClient<Curator5ZookeeperClient.NodeCacheListenerImpl, Curator5ZookeeperClient.CuratorWatcherImpl> {

    protected static final Logger logger = LoggerFactory.getLogger(Curator5ZookeeperClient.class);
    private static final String ZK_SESSION_EXPIRE_KEY = "zk.session.expire";

    private static final Charset CHARSET = StandardCharsets.UTF_8;
    private final CuratorFramework client;
    private static Map<String, NodeCache> nodeCacheMap = new ConcurrentHashMap<>();

    public Curator5ZookeeperClient(URL url) {
        super(url);
        try {
            // 对zk发起连接超时的时间默认是5s，尝试向zk发起连接，如果超过5s没连接上去，此时就超时了
            int timeout = url.getParameter(TIMEOUT_KEY, DEFAULT_CONNECTION_TIMEOUT_MS);
            // session会话过期的时间，默认是1分钟，如果zk客户端和zk服务端连接断开了超过1分钟，此时会话过期
            int sessionExpireMs = url.getParameter(ZK_SESSION_EXPIRE_KEY, DEFAULT_SESSION_TIMEOUT_MS);
            // 基于curator框架去构建zk client，curator是zk在使用的时候，最常用的一个框架
            // curator框架是对zk原生client做了一层包装
            // 访问redis，redisson框架，封装了redis原生Jedis API，提供了大量高阶的功能
            CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder()
                    .connectString(url.getBackupAddress())
                    .retryPolicy(new RetryNTimes(1, 1000)) // 重试的时候，间隔1s，发起1次重试
                    .connectionTimeoutMs(timeout) // 如果尝试连接超过5s都没连上的话，此时的话就会进行重试
                    .sessionTimeoutMs(sessionExpireMs); // 会话过期的时间，断开连接超过1分钟，此时会话就会过期
            String authority = url.getAuthority();
            if (authority != null && authority.length() > 0) {
                builder = builder.authorization("digest", authority.getBytes());
            }
            client = builder.build();
            client.getConnectionStateListenable().addListener(new CuratorConnectionStateListener(url));
            client.start();
            // 阻塞住一直到跟zk成功建立连接为止
            boolean connected = client.blockUntilConnected(timeout, TimeUnit.MILLISECONDS);
            if (!connected) {
                throw new IllegalStateException("zookeeper not connected");
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void createPersistent(String path) {
        try {
            client.create().forPath(path);
        } catch (NodeExistsException e) {
            logger.warn("ZNode " + path + " already exists.", e);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void createEphemeral(String path) {
        try {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
        }
        // node已经存在了，你注册的服务实例，之前已经注册过了，你就不能重复注册
        catch (NodeExistsException e) {
            logger.warn("ZNode " + path + " already exists, since we will only try to recreate a node on a session expiration" +
                    ", this duplication might be caused by a delete delay from the zk server, which means the old expired session" +
                    " may still holds this ZNode and the server just hasn't got time to do the deletion. In this case, " +
                    "we can just try to delete and create again.", e);
            // 如果有一个重复注册的过程，此时怎么处理，先删除那个znode，再去重新创建一个临时节点
            deletePath(path);
            createEphemeral(path);
        } catch (Exception e) {
            // 创建过程中，如果说跟zk有链接问题，或者是zk自己有问题，此时就会出事儿
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void createPersistent(String path, String data) {
        byte[] dataBytes = data.getBytes(CHARSET);
        try {
            client.create().forPath(path, dataBytes);
        } catch (NodeExistsException e) {
            try {
                client.setData().forPath(path, dataBytes);
            } catch (Exception e1) {
                throw new IllegalStateException(e.getMessage(), e1);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void createEphemeral(String path, String data) {
        byte[] dataBytes = data.getBytes(CHARSET);
        try {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(path, dataBytes);
        } catch (NodeExistsException e) {
            logger.warn("ZNode " + path + " already exists, since we will only try to recreate a node on a session expiration" +
                    ", this duplication might be caused by a delete delay from the zk server, which means the old expired session" +
                    " may still holds this ZNode and the server just hasn't got time to do the deletion. In this case, " +
                    "we can just try to delete and create again.", e);
            deletePath(path);
            createEphemeral(path, data);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void update(String path, String data, int version) {
        byte[] dataBytes = data.getBytes(CHARSET);
        try {
            client.setData().withVersion(version).forPath(path, dataBytes);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void createOrUpdatePersistent(String path, String data, int version) {
        try {
            if (checkExists(path)) {
                update(path, data, version);
            } else {
                createPersistent(path, data);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void createOrUpdateEphemeral(String path, String data, int version) {
        try {
            if (checkExists(path)) {
                update(path, data, version);
            } else {
                createEphemeral(path, data);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected void deletePath(String path) {
        try {
            client.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (NoNodeException ignored) {
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public List<String> getChildren(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean checkExists(String path) {
        try {
            if (client.checkExists().forPath(path) != null) {
                return true;
            }
        } catch (Exception ignored) {
        }
        return false;
    }

    @Override
    public boolean isConnected() {
        return client.getZookeeperClient().isConnected();
    }

    @Override
    public String doGetContent(String path) {
        try {
            byte[] dataBytes = client.getData().forPath(path);
            return (dataBytes == null || dataBytes.length == 0) ? null : new String(dataBytes, CHARSET);
        } catch (NoNodeException e) {
            // ignore NoNode Exception.
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public ConfigItem doGetConfigItem(String path) {
        String content;
        Stat stat;
        try {
            stat = new Stat();
            byte[] dataBytes = client.getData().storingStatIn(stat).forPath(path);
            content = (dataBytes == null || dataBytes.length == 0) ? null : new String(dataBytes, CHARSET);
        } catch (NoNodeException e) {
            return new ConfigItem();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
        return new ConfigItem(content, stat);
    }

    @Override
    public void doClose() {
        super.doClose();
        client.close();
    }

    @Override
    public Curator5ZookeeperClient.CuratorWatcherImpl createTargetChildListener(String path, ChildListener listener) {
        return new Curator5ZookeeperClient.CuratorWatcherImpl(client, listener, path);
    }

    @Override
    public List<String> addTargetChildListener(String path, CuratorWatcherImpl listener) {
        try {
            return client.getChildren().usingWatcher(listener).forPath(path);
        } catch (NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    protected Curator5ZookeeperClient.NodeCacheListenerImpl createTargetDataListener(String path, DataListener listener) {
        return new NodeCacheListenerImpl(client, listener, path);
    }

    @Override
    protected void addTargetDataListener(String path, Curator5ZookeeperClient.NodeCacheListenerImpl nodeCacheListener) {
        this.addTargetDataListener(path, nodeCacheListener, null);
    }

    @Override
    protected void addTargetDataListener(String path, Curator5ZookeeperClient.NodeCacheListenerImpl nodeCacheListener, Executor executor) {
        try {
            NodeCache nodeCache = new NodeCache(client, path);
            if (nodeCacheMap.putIfAbsent(path, nodeCache) != null) {
                return;
            }
            if (executor == null) {
                nodeCache.getListenable().addListener(nodeCacheListener);
            } else {
                nodeCache.getListenable().addListener(nodeCacheListener, executor);
            }

            // 大概可以做一个推测，他肯定是说，把你的监听器施加操作，在你的内存里做一个缓存
            // 再通过一个线程池提交异步任务，异步化的去做施加监听器的操作

            nodeCache.start();
        } catch (Exception e) {
            throw new IllegalStateException("Add nodeCache listener for path:" + path, e);
        }
    }

    @Override
    protected void removeTargetDataListener(String path, Curator5ZookeeperClient.NodeCacheListenerImpl nodeCacheListener) {
        NodeCache nodeCache = nodeCacheMap.get(path);
        if (nodeCache != null) {
            nodeCache.getListenable().removeListener(nodeCacheListener);
        }
        nodeCacheListener.dataListener = null;
    }

    @Override
    public void removeTargetChildListener(String path, CuratorWatcherImpl listener) {
        listener.unwatch();
    }

    static class NodeCacheListenerImpl implements NodeCacheListener {

        private CuratorFramework client;

        private volatile DataListener dataListener;

        private String path;

        protected NodeCacheListenerImpl() {
        }

        public NodeCacheListenerImpl(CuratorFramework client, DataListener dataListener, String path) {
            this.client = client;
            this.dataListener = dataListener;
            this.path = path;
        }

        @Override
        public void nodeChanged() throws Exception {
            // 如果说有对应的节点的值的变化，此时就会回调这里
            ChildData childData = nodeCacheMap.get(path).getCurrentData();
            String content = null;
            EventType eventType;
            if (childData == null) {
                eventType = EventType.NodeDeleted; // 就说明出现了一个node delete删除事件
            } else if (childData.getStat().getVersion() == 0) {
                content = new String(childData.getData(), CHARSET);
                eventType = EventType.NodeCreated; // 就说明有一个node创建的一个事件
            } else {
                content = new String(childData.getData(), CHARSET);
                eventType = EventType.NodeDataChanged; // node数据值的变化
            }
            dataListener.dataChanged(path, content, eventType);
        }
    }

    static class CuratorWatcherImpl implements CuratorWatcher {

        private CuratorFramework client;
        private volatile ChildListener childListener;
        private String path;

        public CuratorWatcherImpl(CuratorFramework client, ChildListener listener, String path) {
            this.client = client;
            this.childListener = listener;
            this.path = path;
        }

        protected CuratorWatcherImpl() {
        }

        public void unwatch() {
            this.childListener = null;
        }

        @Override
        public void process(WatchedEvent event) throws Exception {
            // if client connect or disconnect to server, zookeeper will queue
            // watched event(Watcher.Event.EventType.None, .., path = null).
            if (event.getType() == Watcher.Event.EventType.None) {
                return;
            }

            if (childListener != null) {
                childListener.childChanged(path, client.getChildren().usingWatcher(this).forPath(path));
            }
        }
    }

    private class CuratorConnectionStateListener implements ConnectionStateListener {
        private final long UNKNOWN_SESSION_ID = -1L;

        private long lastSessionId;
        private int timeout;
        private int sessionExpireMs;

        public CuratorConnectionStateListener(URL url) {
            this.timeout = url.getParameter(TIMEOUT_KEY, DEFAULT_CONNECTION_TIMEOUT_MS);
            this.sessionExpireMs = url.getParameter(ZK_SESSION_EXPIRE_KEY, DEFAULT_SESSION_TIMEOUT_MS);
        }

        // 跟zk的连接建立了之后，一般来说你都得关注一下跟这个zk之间的连接
        // 如果跟zk的连接有断开，此时会回调通知你的
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState state) {
            long sessionId = UNKNOWN_SESSION_ID;
            try {
                sessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
            } catch (Exception e) {
                logger.warn("Curator client state changed, but failed to get the related zk session instance.");
            }

            if (state == ConnectionState.LOST) {
                logger.warn("Curator zookeeper session " + Long.toHexString(lastSessionId) + " expired.");
                Curator5ZookeeperClient.this.stateChanged(StateListener.SESSION_LOST);
            } else if (state == ConnectionState.SUSPENDED) {
                logger.warn("Curator zookeeper connection of session " + Long.toHexString(sessionId) + " timed out. " +
                        "connection timeout value is " + timeout + ", session expire timeout value is " + sessionExpireMs);
                Curator5ZookeeperClient.this.stateChanged(StateListener.SUSPENDED);
            } else if (state == ConnectionState.CONNECTED) {
                lastSessionId = sessionId;
                logger.info("Curator zookeeper client instance initiated successfully, session id is " + Long.toHexString(sessionId));
                Curator5ZookeeperClient.this.stateChanged(StateListener.CONNECTED);
            } else if (state == ConnectionState.RECONNECTED) {
                if (lastSessionId == sessionId && sessionId != UNKNOWN_SESSION_ID) {
                    logger.warn("Curator zookeeper connection recovered from connection lose, " +
                            "reuse the old session " + Long.toHexString(sessionId));
                    Curator5ZookeeperClient.this.stateChanged(StateListener.RECONNECTED);
                } else {
                    logger.warn("New session created after old session lost, " +
                            "old session " + Long.toHexString(lastSessionId) + ", new session " + Long.toHexString(sessionId));
                    lastSessionId = sessionId;
                    Curator5ZookeeperClient.this.stateChanged(StateListener.NEW_SESSION_CREATED);
                }
            }
        }

    }

    /**
     * just for unit test
     *
     * @return
     */
    CuratorFramework getClient() {
        return client;
    }
}
