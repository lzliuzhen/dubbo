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
package org.apache.dubbo.rpc.model;

import org.apache.dubbo.common.extension.ExtensionPostProcessor;

public class ScopeModelAwareExtensionProcessor implements ExtensionPostProcessor, ScopeModelAccessor {
    private ScopeModel scopeModel;
    private FrameworkModel frameworkModel;
    private ApplicationModel applicationModel;
    private ModuleModel moduleModel;

    public ScopeModelAwareExtensionProcessor(ScopeModel scopeModel) {
        this.scopeModel = scopeModel;
        initialize();
    }

    private void initialize() {

        // NOTE: Do not create a new model or use the default application/module model here!
        // Only the visible and only matching scope model can be injected, that is, module -> application -> framework.
        // The converse is a one-to-many relationship and cannot be injected.
        // One framework may have multiple applications, and one application may have multiple modules.
        // So, the spi extension/bean of application scope can be injected it's application model and framework model,
        // but the spi extension/bean of framework scope cannot be injected an application or module model.

        if (scopeModel instanceof FrameworkModel) {
            frameworkModel = (FrameworkModel) scopeModel;
        } else if (scopeModel instanceof ApplicationModel) {
            applicationModel = (ApplicationModel) scopeModel;
            frameworkModel = applicationModel.getFrameworkModel();
        } else if (scopeModel instanceof ModuleModel) {
            moduleModel = (ModuleModel) scopeModel;
            applicationModel = moduleModel.getApplicationModel();
            frameworkModel = applicationModel.getFrameworkModel();
        }
    }

    @Override
    // 后续你的model组件通过SPI机制创建出了一个extension实例之后
    // 会进行回调和后处理，回调你的scope model的后处理监听器，extension实例交给你，让你进行处理
    public Object postProcessAfterInitialization(Object instance, String name) throws Exception {
        if (instance instanceof ScopeModelAware) {
            // 在这里，所谓的处理，很简单，无法就是让创建出的extension实例，如果他要是实现了ScopeModelAware接口
            // 那个SPI的extension实例，是需要感知到对应的model组件
            // 就会把model组件自己设置到你的extension实例里去，让你的extension实例感知到你
            ScopeModelAware modelAware = (ScopeModelAware) instance;
            modelAware.setScopeModel(scopeModel);
            if (this.moduleModel != null) {
                modelAware.setModuleModel(this.moduleModel);
            }
            if (this.applicationModel != null) {
                modelAware.setApplicationModel(this.applicationModel);
            }
            if (this.frameworkModel != null) {
                modelAware.setFrameworkModel(this.frameworkModel);
            }
        }
        return instance;
    }

    @Override
    public ScopeModel getScopeModel() {
        return scopeModel;
    }

    @Override
    public FrameworkModel getFrameworkModel() {
        return frameworkModel;
    }

    @Override
    public ApplicationModel getApplicationModel() {
        return applicationModel;
    }

    @Override
    public ModuleModel getModuleModel() {
        return moduleModel;
    }
}
