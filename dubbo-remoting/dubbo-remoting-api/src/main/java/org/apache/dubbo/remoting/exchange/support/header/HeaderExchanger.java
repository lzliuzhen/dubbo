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
package org.apache.dubbo.remoting.exchange.support.header;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.Transporters;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.ExchangeServer;
import org.apache.dubbo.remoting.exchange.Exchanger;
import org.apache.dubbo.remoting.transport.DecodeHandler;

/**
 * DefaultMessenger
 *
 *
 */
public class HeaderExchanger implements Exchanger {

    public static final String NAME = "header";

    // NettyClient -> MultiMessageHandler -> HeartbeatHandler -> AllChannelHandler -> DecodeHandler -> HeaderExchangeHandler -> requestHandler
    // HeaderExchangeClient -> NettyClient
    @Override
    public ExchangeClient connect(URL url, ExchangeHandler handler) throws RemotingException {
        return new HeaderExchangeClient(Transporters.connect(url, new DecodeHandler(new HeaderExchangeHandler(handler))), true);
    }

    @Override
    public ExchangeServer bind(URL url, ExchangeHandler handler) throws RemotingException {
        // exchanger这一层，是属于让我们的上层的代码，把一些rpc invocation调用，转为请求/响应的模型，同步转异步
        // 网络模型了，请求概念
        // 必然是要把请求通过底层的网络框架发送出去的
        // 在这个底层，你要获取到网络框架实现的server和client，去封装给exchanger组件底层里面去
        // 不同的Transporter
        // 在exchanger里面可以使用不同的网络技术的，netty、mina，这些都是属于网络通信的框架
        // 对不同的网络技术，如果要把他们进行统一的封装，netty、mina不同的框架，他的用法和API都是不同的
        // exchanger这一层，如果你把netty、mina他直接的API提供过去
        // 必须要做一个Transporter这一层，封装、抽象和统一底层的网络框架的使用模型和标准
        // exchanger这一层，直接基于transporter这一层 提供的标准模型来使用就可以了
        return new HeaderExchangeServer(Transporters.bind(url, new DecodeHandler(new HeaderExchangeHandler(handler))));
    }

}
