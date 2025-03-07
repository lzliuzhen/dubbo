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
package org.apache.dubbo.common.beans;

import org.apache.dubbo.common.beans.factory.ScopeBeanFactory;
import org.apache.dubbo.common.extension.ExtensionInjector;
import org.apache.dubbo.rpc.model.ScopeModel;
import org.apache.dubbo.rpc.model.ScopeModelAware;

/**
 * Inject scope bean to SPI extension instance
 */
public class ScopeBeanExtensionInjector implements ExtensionInjector, ScopeModelAware {
    private ScopeModel scopeModel;
    private ScopeBeanFactory beanFactory;

    @Override
    public void setScopeModel(ScopeModel scopeModel) {
        this.scopeModel = scopeModel;
        this.beanFactory = scopeModel.getBeanFactory();
    }

    @Override
    public <T> T getInstance(Class<T> type, String name) {
        return beanFactory.getBean(name, type); // 还可以根据容器，根据接口类型，名称，protocol
    }
}
