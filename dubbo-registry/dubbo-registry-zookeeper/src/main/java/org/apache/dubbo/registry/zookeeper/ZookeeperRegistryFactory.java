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
package org.apache.dubbo.registry.zookeeper;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.DisableInject;
import org.apache.dubbo.registry.Registry;
import org.apache.dubbo.registry.support.AbstractRegistryFactory;
import org.apache.dubbo.remoting.zookeeper.ZookeeperTransporter;
import org.apache.dubbo.rpc.model.ApplicationModel;

/**
 * ZookeeperRegistryFactory.
 */
public class ZookeeperRegistryFactory extends AbstractRegistryFactory {

    // 跟zk之间的网络通信的组件
    private ZookeeperTransporter zookeeperTransporter;

    private ApplicationModel applicationModel;

    // for compatible usage
    public ZookeeperRegistryFactory() {
        this(ApplicationModel.defaultModel());
    }

    public ZookeeperRegistryFactory(ApplicationModel applicationModel) {
        this.applicationModel = applicationModel;
        // 通过SPI机制来进行获取
        this.zookeeperTransporter = ZookeeperTransporter.getExtension(applicationModel);
    }

    @DisableInject
    public void setZookeeperTransporter(ZookeeperTransporter zookeeperTransporter) {
        this.zookeeperTransporter = zookeeperTransporter;
    }

    @Override
    public Registry createRegistry(URL url) {
        // 无非就是创建一个ZooKeeperRegistry，注册中心组件
        // 基于zk的注册中心组件，值得我们接下来下一讲好好的来分析一下
        return new ZookeeperRegistry(url, zookeeperTransporter);
    }

}
