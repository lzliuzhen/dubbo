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
package org.apache.dubbo.common.beans.factory;

import org.apache.dubbo.common.beans.ScopeBeanException;
import org.apache.dubbo.common.beans.support.InstantiationStrategy;
import org.apache.dubbo.common.extension.ExtensionAccessor;
import org.apache.dubbo.common.extension.ExtensionAccessorAware;
import org.apache.dubbo.common.extension.ExtensionPostProcessor;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.model.ScopeModelAccessor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A bean factory for internal sharing.
 *
 * 他是dubbo框架自己内部实现的一个bean工厂，bean工厂是管理的这些bean是用于在dubbo框架内部进行共享的
 * 非常值得我们下一讲，深入的来讲解一下
 *
 * model组件体系、结合实际使用案例的SPI扩展机制的实现、dubbo内部的bean容器
 *
 */
public class ScopeBeanFactory {

    // factory都是可以挂父级组件的，组成一颗树
    private final ScopeBeanFactory parent;
    // extension扩展实例的获取组件，有了这个东西，就有了对SPI机制使用的权限
    private ExtensionAccessor extensionAccessor;
    // extension实例后处理器
    private List<ExtensionPostProcessor> extensionPostProcessors;
    // 每一个class都有一个AtomicInteger作为一个计数器
    private Map<Class, AtomicInteger> beanNameIdCounterMap = new ConcurrentHashMap<>();
    // 注册过的bean实例信息
    private List<BeanInfo> registeredBeanInfos = Collections.synchronizedList(new ArrayList<>());
    // 初始化策略逻辑，bean实例的初始化
    private InstantiationStrategy instantiationStrategy;

    public ScopeBeanFactory(ScopeBeanFactory parent, ExtensionAccessor extensionAccessor) {
        this.parent = parent;
        this.extensionAccessor = extensionAccessor;
        extensionPostProcessors = extensionAccessor.getExtensionDirector().getExtensionPostProcessors();
        initInstantiationStrategy();
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

    // 你是bean管理的容器
    public <T> T registerBean(Class<T> bean) throws ScopeBeanException {
        // 默认来说name可以是null
        return this.getOrRegisterBean(null, bean);
    }

    // name也是可以自己设置的
    public <T> T registerBean(String name, Class<T> clazz) throws ScopeBeanException {
        return getOrRegisterBean(name, clazz);
    }

    private <T> T createAndRegisterBean(String name, Class<T> clazz) {
        T instance = getBean(name, clazz);
        if (instance != null) {
            throw new ScopeBeanException("already exists bean with same name and type, name=" + name + ", type=" + clazz.getName());
        }
        try {
            // 直接就用你的实现类的class，初始化了一个实例对象
            instance = instantiationStrategy.instantiate(clazz);
        } catch (Throwable e) {
            throw new ScopeBeanException("create bean instance failed, type=" + clazz.getName(), e);
        }
        // 实例化一个bean是很简单的
        registerBean(name, instance);
        return instance;
    }

    public void registerBean(Object bean) {
        this.registerBean(null, bean);
    }

    public void registerBean(String name, Object bean) {
        // avoid duplicated register same bean
        if (containsBean(name, bean)) {
            return;
        }

        Class<?> beanClass = bean.getClass();
        if (name == null) {
            name = beanClass.getName() + "#" + getNextId(beanClass);
        }
        initializeBean(name, bean);

        registeredBeanInfos.add(new BeanInfo(name, bean));
    }

    public <T> T getOrRegisterBean(Class<T> type) {
        return getOrRegisterBean(null, type);
    }

    public <T> T getOrRegisterBean(String name, Class<T> type) {
        T bean = getBean(name, type);
        if (bean == null) {
            // lock by type
            synchronized (type) {
                bean = getBean(name, type);
                if (bean == null) {
                    bean = createAndRegisterBean(name, type);
                }
            }
        }
        return bean;
    }

    public <T> T getOrRegisterBean(Class<T> type, Function<? super Class<T>, ? extends T> mappingFunction) {
        return getOrRegisterBean(null, type, mappingFunction);
    }

    public <T> T getOrRegisterBean(String name, Class<T> type, Function<? super Class<T>, ? extends T> mappingFunction) {
        T bean = getBean(name, type);
        if (bean == null) {
            // lock by type
            synchronized (type) {
                bean = getBean(name, type);
                if (bean == null) {
                    bean = mappingFunction.apply(type);
                    registerBean(name, bean);
                }
            }
        }
        return bean;
    }

    public <T> T initializeBean(T bean) {
        this.initializeBean(null, bean);
        return bean;
    }

    private void initializeBean(String name, Object bean) {
        try {
            if (bean instanceof ExtensionAccessorAware) {
                ((ExtensionAccessorAware) bean).setExtensionAccessor(extensionAccessor);
            }
            for (ExtensionPostProcessor processor : extensionPostProcessors) {
                processor.postProcessAfterInitialization(bean, name);
            }
        } catch (Exception e) {
            throw new ScopeBeanException("register bean failed! name=" + name + ", type=" + bean.getClass().getName(), e);
        }
    }

    private boolean containsBean(String name, Object bean) {
        for (BeanInfo beanInfo : registeredBeanInfos) {
            if (beanInfo.instance == bean &&
                (name == null || StringUtils.isEquals(name, beanInfo.name))) {
                return true;
            }
        }
        return false;
    }

    private int getNextId(Class<?> beanClass) {
        return beanNameIdCounterMap.computeIfAbsent(beanClass, key -> new AtomicInteger()).incrementAndGet();
    }

    public <T> T getBean(Class<T> type) {
        return this.getBean(null, type);
    }

    public <T> T getBean(String name, Class<T> type) {
        T bean = getBeanInternal(name, type);
        if (bean == null && parent != null) {
            return parent.getBean(name, type);
        }
        return bean;
    }

    private <T> T getBeanInternal(String name, Class<T> type) {
        // All classes are derived from java.lang.Object, cannot filter bean by it
        if (type == Object.class) {
            return null;
        }
        List<BeanInfo> candidates = null;
        BeanInfo firstCandidate = null;

        // type就是属于你的bean的class类型
        for (BeanInfo beanInfo : registeredBeanInfos) {
            // if required bean type is same class/superclass/interface of the registered bean
            if (type.isAssignableFrom(beanInfo.instance.getClass())) {
                if (StringUtils.isEquals(beanInfo.name, name)) {
                    return (T) beanInfo.instance;
                } else {
                    // optimize for only one matched bean
                    if (firstCandidate == null) {
                        firstCandidate = beanInfo;
                    } else {
                        if (candidates == null) {
                            candidates = new ArrayList<>();
                            candidates.add(firstCandidate);
                        }
                        candidates.add(beanInfo);
                    }
                }
            }
        }

        // if bean name not matched and only single candidate
        if (candidates != null) {
            if (candidates.size() == 1) {
                return (T) candidates.get(0).instance;
            } else if (candidates.size() > 1) {
                List<String> candidateBeanNames = candidates.stream().map(beanInfo -> beanInfo.name).collect(Collectors.toList());
                throw new ScopeBeanException("expected single matching bean but found " + candidates.size() + " candidates for type [" + type.getName() + "]: " + candidateBeanNames);
            }
        } else if (firstCandidate != null) {
            return (T) firstCandidate.instance;
        }
        return null;
    }

    static class BeanInfo {
        // bean名称和一个bean实例
        private String name;
        private Object instance;

        public BeanInfo(String name, Object instance) {
            this.name = name;
            this.instance = instance;
        }
    }
}
