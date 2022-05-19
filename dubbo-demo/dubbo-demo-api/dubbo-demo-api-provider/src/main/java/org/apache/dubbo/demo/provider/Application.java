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
package org.apache.dubbo.demo.provider;

import org.apache.dubbo.config.*;
import org.apache.dubbo.config.bootstrap.DubboBootstrap;
import org.apache.dubbo.demo.DemoService;

import java.util.concurrent.CountDownLatch;

public class Application {
    public static void main(String[] args) throws Exception {
        if (isClassic(args)) {
            startWithExport();
        } else {
            startWithBootstrap();
        }
    }

    private static boolean isClassic(String[] args) {
        return args.length > 0 && "classic".equalsIgnoreCase(args[0]);
    }

    private static void startWithBootstrap() {
        ServiceConfig<DemoServiceImpl> service = new ServiceConfig<>();
        service.setInterface(DemoService.class);
        service.setRef(new DemoServiceImpl());

        DubboBootstrap bootstrap = DubboBootstrap.getInstance();
        bootstrap.application(new ApplicationConfig("dubbo-demo-api-provider"))
            .registry(new RegistryConfig("zookeeper://127.0.0.1:2181"))
            .service(service)
            .start()
            .await();
    }

    // 第一个why的问题：dubbo里为什么要设计provider和consumer两种角色？
    // provider和consumer是代表了dubbo里服务实例中不同的两种角色，是在里面的

    // service和reference两个概念，没有，合一
    // ServiceInstance概念，一个一个的服务实例，每个服务实例都可以对外提供provider的服务暴露的功能
    // 也可以去找别人去调用扮演一个consumer的角色，消费的角色
    // 大脑设想一下，会有什么问题，ServiceInstance里面，耦合了暴露服务和调用服务两个截然不同的功能场景
    // 必然会导致ServiceInstance里面的代码会膨胀的非常厉害，导致不同的功能场景的代码，耦合严重
    // 写代码，最怕的就是没太大关联关系的一些代码，耦合再一起，这会导致极为严重的不好维护

    // dubbo里面是采取了设计思想：对服务实例的服务暴露和服务调用两个功能场景，进行了拆分、解耦和隔离
    // 非常优秀的一个设计思想，你没有写过大量的中间件的代码，或者是没看过大量的中间件的源码，你可能很难体会到
    // 服务实例的角色是进行了分离，一个服务实例可以扮演多种角色，他可以暴露服务的角色，也可以扮演对多个不同服务的服务调用的角色
    // 就可以把不同的功能场景的代码做一个隔离

    // ServiceConfig -> 扮演的就是一个服务暴露的角色和功能
    // ReferenceConfig -> 扮演的就是一个服务调用的角色和功能
    // 比如说你现在有一个dubbo的服务实例，启动的时候可以拥有多种不同的角色的，他自己就可以对外暴露服务 -> ServiceConfig
    // 他自己也可以去依赖和调用多个其他的服务，就可以有多个ReferenceConfig的概念，每个ReferenceConfig都代表和扮演了对一个服务实例的调用
    // 不同功能场景的角色的代码，都放在自己的XxConfig里面的

    private static void startWithExport() throws InterruptedException {
        // 在整个启动和初始化的过程之中，ScopeModel、ApplicationModel，都是一些核心以及重要的组件
        // 这些组件又可以通过他们去获取更多的组件，ConfigManager、Repository
        // xxModel一类的组件体系，是我们非常关键的，应该先去搞明白细节的一些类和组件体系
        // 其次，搞明白这些东西之后，我们还应该去搞明白SPI机制，我们配合组件对SPI机制的使用
        // 大量的运用SPI机制，去拿接口的扩展实现类的实例，dubbo源码里，真实的对SPI机制的运用，细扣SPI扩展机制的实现细节

//        ConfigCenterConfig configCenter = new ConfigCenterConfig();
//        configCenter.setAddress("zookeeper://127.0.0.1:2181");

        // ServiceConfig到底是什么东西？
        // Service是什么东西，定义成一个服务，每个服务可以包含多个接口
        // ServiceConfig顾名思义，针对这个dubbo服务的一些配置信息
        // 泛型里包含的这个DemoServiceImpl是什么东西，服务的接口必须有实现代码，DemoServiceImpl=服务接口的实现代码
        ServiceConfig<DemoServiceImpl> service = new ServiceConfig<>();
//        service.setConfigCenter(configCenter);
        // 设置你的服务暴露出去的接口
        service.setInterface(DemoService.class);
        // 进一步明确设置你的暴露出去的接口的实现代码
        service.setRef(new DemoServiceImpl());
        // application name，在服务框架里，定位都是你的服务名称
        service.setApplication(new ApplicationConfig("dubbo-demo-api-provider"));
        // 所有的rpc框架，必须要跟注册中心配合使用，服务启动之后必须向注册中心进行注册
        // 注册中心是知道每一个服务有几个实例，每个实例在哪台服务器上
        // 其他的服务，就必须找注册中心，询问我要调用的服务有几个实例，分别再什么机器上
        // 设置zookeeper作为注册中心的地址
        service.setRegistry(new RegistryConfig("zookeeper://127.0.0.1:2181"));
        // 元数据上报配置，dubbo服务实例启动之后，肯定会有自己的元数据，必须上报到zk上去
        service.setMetadataReportConfig(new MetadataReportConfig("zookeeper://127.0.0.1:2181"));
        // 都配置完毕之后，走一个export方法，推断，就一定会有网络监听的程序必须会启动
        // 别人调用你，需要跟你建立网络连接，再进行网络通信，按照协议，把请求数据发送给你，执行rpc调用
        // 把自己作为一个服务实例注册到zk里去
        service.export();

        System.out.println("dubbo service started");
        new CountDownLatch(1).await();
    }
}
