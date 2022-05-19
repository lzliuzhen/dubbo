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

import org.apache.dubbo.common.beans.factory.ScopeBeanFactory;
import org.apache.dubbo.common.config.Environment;
import org.apache.dubbo.common.extension.ExtensionAccessor;
import org.apache.dubbo.common.extension.ExtensionDirector;
import org.apache.dubbo.common.extension.ExtensionScope;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 他是model组件体系里，最顶层的抽象的父类
 * 他实现了一个很关键的一个接口，ExtensionAccessor接口，很有意思，大家一定是看不懂的
 * ExtensionAccessor，extension（SPI扩展实现）的获取组件
 * 是不是在dubbo组件体系里，如果你要实现了extension accessor接口之后，就代表你具备了使用
 * dubbo的SPI机制，随时可以获取指定接口的扩展实现的能力
 *
 * ScopeModel，最关键的一些关联性的东西，是谁呢，SPI机制，extension scope，extension director
 * bean factory
 *
 */
public abstract class ScopeModel implements ExtensionAccessor {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ScopeModel.class);

    /**
     * The internal name is used to represent the hierarchy of the model tree, such as:
     * <ol>
     *     <li>FrameworkModel-1</li>
     *     FrameworkModel (index=1)
     *     <li>ApplicationModel-1.2</li>
     *     FrameworkModel (index=1) -> ApplicationModel (index=2)
     *     <li>ModuleModel-1.2.0</li>
     *     FrameworkModel (index=1) -> ApplicationModel (index=2) -> ModuleModel (index=0, internal module)
     *     <li>ModuleModel-1.2.1</li>
     *     FrameworkModel (index=1) -> ApplicationModel (index=2) -> ModuleModel (index=1, first user module)
     * </ol>
     */
    private String internalName; // 内部名称，根据不同的名称，代表了一个不同的model组件体系

    /**
     * Public Model Name, can be set from user
     */
    private String modelName;
    // 一批要使用的类加载器
    private Set<ClassLoader> classLoaders;
    // 这里有一个parent，这个太关键了，类型也是ScopeModel，model组件体系必然会基于parent属性，构建成一个树的结构
    private final ScopeModel parent;
    // 一看就是跟SPI机制的使用，是有关系的，他是一个枚举类型，代表的是说，你在这里使用SPI机制的一个范围是什么
    // 你有很多的model组件，不同的extension scope范围，就决定了你创建出的extension实例，到底是在一个model组件里可以用
    // 还是可以跟其他的model组件可以进行共享使用，或者是别的model组件创建的extension实例就是新的了
    private final ExtensionScope scope;
    // 我们刚才已经看到了，extension director，本质来说就是一个extension loader的manager，管理组件
    // 针对某个接口去加载到他对应的SPI扩展实例，就必须先通过ExtensionDirector，先获取到那个接口对应的ExtensionLoader组件
    // 通过ExtensionLoader组件再去获取对应的Extension对象实例
    private ExtensionDirector extensionDirector;
    // socpe bean factory，spring容器很像了，bean这个概念，scope factory工厂的概念再里面
    private ScopeBeanFactory beanFactory;
    // model组件，有自己的生命周期，创建、使用、销毁，如果说model组件有销毁的行为的话，此时可以在销毁之前
    // 先回调你的销毁事件的监听器，这样的话就有一个model组件的生命周期的事件监听机制在里面
    private List<ScopeModelDestroyListener> destroyListeners;
    // model组件有关联的一些属性数据
    private Map<String, Object> attributes;
    // jdk并发包里提供的一个Atomic变量，destroyed，model组件是否被销毁了
    private final AtomicBoolean destroyed = new AtomicBoolean(false);

    // 构造函数里，可以支持传入他对应的parent，通过parent变量可以把model体系组装成一个树
    // 传入一个ExtensionScope，这个model组件里创建的extension实例，他的使用范围是什么
    public ScopeModel(ScopeModel parent, ExtensionScope scope) {
        this.parent = parent;
        this.scope = scope;
    }

    /**
     * NOTE:
     * <ol>
     *  <li>The initialize method only be called in subclass.</li>
     * <li>
     * In subclass, the extensionDirector and beanFactory are available in initialize but not available in constructor.
     * </li>
     * </ol>
     */
    protected void initialize() {
        // 初始化获取extension director，SPI核心机制的manager组件
        this.extensionDirector = new ExtensionDirector(parent != null ? parent.getExtensionDirector() : null, scope, this);
        // SPI机制在创建和获取extension实例的时候，看到过源码，post processor，前处理和后处理的过程
        this.extensionDirector.addExtensionPostProcessor(new ScopeModelAwareExtensionProcessor(this));
        // scope bean fctory，构建出了一个dubbo内部的bean容器，小型的bean容器
        this.beanFactory = new ScopeBeanFactory(parent != null ? parent.getBeanFactory() : null, extensionDirector);
        this.destroyListeners = new LinkedList<>();
        this.attributes = new ConcurrentHashMap<>();
        this.classLoaders = new ConcurrentHashSet<>();

        // Add Framework's ClassLoader by default
        ClassLoader dubboClassLoader = ScopeModel.class.getClassLoader();
        if (dubboClassLoader != null) {
            this.addClassLoader(dubboClassLoader);
        }
    }

    // model组件要被销毁了
    public void destroy() {
        // cas操作，多线程并发来访问这个方法，是线程安全的，boolean值的改变，是不会出错的
        if (destroyed.compareAndSet(false, true)) {
            try {
                onDestroy();
                HashSet<ClassLoader> copyOfClassLoaders = new HashSet<>(classLoaders);
                for (ClassLoader classLoader : copyOfClassLoaders) {
                    removeClassLoader(classLoader);
                }
            } catch (Throwable t) {
                LOGGER.error("Error happened when destroying ScopeModel.", t);
            }
        }
    }

    public boolean isDestroyed() {
        return destroyed.get();
    }

    protected void notifyDestroy() {
        for (ScopeModelDestroyListener destroyListener : destroyListeners) {
            destroyListener.onDestroy(this);
        }
    }

    protected abstract void onDestroy();

    public final void addDestroyListener(ScopeModelDestroyListener listener) {
        destroyListeners.add(listener);
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public <T> T getAttribute(String key, Class<T> type) {
        return (T) attributes.get(key);
    }

    public Object getAttribute(String key) {
        return attributes.get(key);
    }

    public void setAttribute(String key, Object value) {
        attributes.put(key, value);
    }

    @Override
    public ExtensionDirector getExtensionDirector() {
        return extensionDirector;
    }

    public ScopeBeanFactory getBeanFactory() {
        return beanFactory;
    }

    public ScopeModel getParent() {
        return parent;
    }

    public String getInternalName() {
        return internalName;
    }

    protected void setInternalName(String internalName) {
        this.internalName = internalName;
        this.modelName = internalName;
    }

    public void addClassLoader(ClassLoader classLoader) {
        this.classLoaders.add(classLoader);
        if (parent != null) {
            parent.addClassLoader(classLoader);
        }
        extensionDirector.removeAllCachedLoader();
    }

    public void removeClassLoader(ClassLoader classLoader) {
        if (checkIfClassLoaderCanRemoved(classLoader)) {
            this.classLoaders.remove(classLoader);
            if (parent != null) {
                parent.removeClassLoader(classLoader);
            }
            extensionDirector.removeAllCachedLoader();
        }
    }

    protected boolean checkIfClassLoaderCanRemoved(ClassLoader classLoader) {
        return classLoader != null && !classLoader.equals(ScopeModel.class.getClassLoader());
    }

    public Set<ClassLoader> getClassLoaders() {
        return Collections.unmodifiableSet(classLoaders);
    }

    public abstract Environment getModelEnvironment();

    public String getInternalId() {
        // XxxModule-1.1
        if (this.internalName == null) {
            return null;
        }
        return this.internalName.substring(this.internalName.indexOf('-') + 1);
    }

    protected String buildInternalName(String type, String parentInternalId, long childIndex) {
        // FrameworkModel-1
        // ApplicationModel-1.1
        // ModuleModel-1.1.1
        if (parentInternalId != null) {
            return type + "-" + parentInternalId + "." + childIndex;
        } else {
            return type + "-" + childIndex;
        }
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }
}
