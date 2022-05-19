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
package org.apache.dubbo.auth.filter;

import org.apache.dubbo.auth.Constants;
import org.apache.dubbo.auth.spi.Authenticator;
import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.constants.CommonConstants;
import org.apache.dubbo.common.extension.Activate;
import org.apache.dubbo.rpc.Filter;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.model.ApplicationModel;

/**
 * The ConsumerSignFilter
 *
 * @see org.apache.dubbo.rpc.Filter
 */
@Activate(group = CommonConstants.CONSUMER, value = Constants.SERVICE_AUTH, order = -10000)
public class ConsumerSignFilter implements Filter {
    private ApplicationModel applicationModel;

    public ConsumerSignFilter(ApplicationModel applicationModel) {
        this.applicationModel = applicationModel;
    }

    @Override
    public Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException {
        URL url = invoker.getUrl();
        // 权限认证这一块，默认是false，不需要进行权限认证，如果开启了以后就会启用权限
        boolean shouldAuth = url.getParameter(Constants.SERVICE_AUTH, false);
        if (shouldAuth) {
            // SPI机制，拿到一个实现类，然后再对你的rpc调用做一个鉴权
            // 判断一下你是否有权限去发起一个访问
            Authenticator authenticator = applicationModel.getExtensionLoader(Authenticator.class)
                    .getExtension(url.getParameter(Constants.AUTHENTICATOR, Constants.DEFAULT_AUTHENTICATOR));
            authenticator.sign(invocation, url);
        }
        return invoker.invoke(invocation);
    }
}
