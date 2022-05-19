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

package org.apache.dubbo.rpc.cluster.router.mesh.rule;

import org.apache.dubbo.rpc.cluster.router.mesh.rule.destination.DestinationRule;
import org.apache.dubbo.rpc.cluster.router.mesh.rule.virtualservice.VirtualServiceRule;

import java.util.LinkedList;
import java.util.List;

// 这个东西，本身就代表了我们之前看到的那些配置信息
// 用文本字符串编写的配置信息，此时这些配置被反向推送过来以后
// 就会解析和转换为我们的VsDestinationGroup
public class VsDestinationGroup {

    private String appName;
    private List<VirtualServiceRule> virtualServiceRuleList = new LinkedList<>();
    private List<DestinationRule> destinationRuleList = new LinkedList<>();

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public List<VirtualServiceRule> getVirtualServiceRuleList() {
        return virtualServiceRuleList;
    }

    public void setVirtualServiceRuleList(List<VirtualServiceRule> virtualServiceRuleList) {
        this.virtualServiceRuleList = virtualServiceRuleList;
    }

    public List<DestinationRule> getDestinationRuleList() {
        return destinationRuleList;
    }

    public void setDestinationRuleList(List<DestinationRule> destinationRuleList) {
        this.destinationRuleList = destinationRuleList;
    }
}
