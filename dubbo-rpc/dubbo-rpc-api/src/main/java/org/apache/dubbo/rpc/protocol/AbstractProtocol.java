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
package org.apache.dubbo.rpc.protocol;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.rpc.Exporter;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Protocol;
import org.apache.dubbo.rpc.ProtocolServer;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.model.FrameworkModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;
import org.apache.dubbo.rpc.support.ProtocolUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_SERVER_SHUTDOWN_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.SHUTDOWN_WAIT_KEY;

/**
 * abstract ProtocolSupport.
 */
public abstract class AbstractProtocol implements Protocol, ScopeModelAware {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final Map<String, Exporter<?>> exporterMap = new ConcurrentHashMap<>();

    /**
     * <host:port, ProtocolServer>
     */
    // 这个是属于网络服务器的缓存，key就是host:port，value就是ProtocolServer
    protected final Map<String, ProtocolServer> serverMap = new ConcurrentHashMap<>();

    // TODO SoftReference
    protected final Set<Invoker<?>> invokers = new ConcurrentHashSet<>();

    protected FrameworkModel frameworkModel;

    @Override
    public void setFrameworkModel(FrameworkModel frameworkModel) {
        this.frameworkModel = frameworkModel;
    }

    protected static String serviceKey(URL url) {
        int port = url.getParameter(Constants.BIND_PORT_KEY, url.getPort());
        return serviceKey(port, url.getPath(), url.getVersion(), url.getGroup());
    }

    protected static String serviceKey(int port, String serviceName, String serviceVersion, String serviceGroup) {
        return ProtocolUtils.serviceKey(port, serviceName, serviceVersion, serviceGroup);
    }

    @Override
    public List<ProtocolServer> getServers() {
        return Collections.unmodifiableList(new ArrayList<>(serverMap.values()));
    }

    protected void loadServerProperties(ProtocolServer server) {
        // read and hold config before destroy
        int serverShutdownTimeout = ConfigurationUtils.getServerShutdownTimeout(server.getUrl().getScopeModel());
        server.getAttributes().put(SHUTDOWN_WAIT_KEY, serverShutdownTimeout);
    }

    protected int getServerShutdownTimeout(ProtocolServer server) {
        return (int) server.getAttributes().getOrDefault(SHUTDOWN_WAIT_KEY, DEFAULT_SERVER_SHUTDOWN_TIMEOUT);
    }

    @Override
    public void destroy() {
        for (Invoker<?> invoker : invokers) {
            if (invoker != null) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Destroy reference: " + invoker.getUrl());
                    }
                    invoker.destroy();
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
            }
        }
        invokers.clear();

        exporterMap.forEach((key, exporter)-> {
            if (exporter != null) {
                try {
                    if (logger.isInfoEnabled()) {
                        logger.info("Unexport service: " + exporter.getInvoker().getUrl());
                    }
                    exporter.unexport();
                } catch (Throwable t) {
                    logger.warn(t.getMessage(), t);
                }
            }
        });
        exporterMap.clear();
    }

    @Override
    public <T> Invoker<T> refer(Class<T> type, URL url) throws RpcException {
        return protocolBindingRefer(type, url);
    }

    @Deprecated
    protected abstract <T> Invoker<T> protocolBindingRefer(Class<T> type, URL url) throws RpcException;

    public Map<String, Exporter<?>> getExporterMap() {
        return exporterMap;
    }

    public Collection<Exporter<?>> getExporters() {
        return Collections.unmodifiableCollection(exporterMap.values());
    }
}
