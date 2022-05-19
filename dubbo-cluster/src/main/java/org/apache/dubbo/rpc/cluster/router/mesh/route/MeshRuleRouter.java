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

package org.apache.dubbo.rpc.cluster.router.mesh.route;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.Router;
import org.apache.dubbo.rpc.cluster.router.mesh.rule.VsDestinationGroup;
import org.apache.dubbo.rpc.cluster.router.mesh.rule.destination.DestinationRule;
import org.apache.dubbo.rpc.cluster.router.mesh.rule.destination.DestinationRuleSpec;
import org.apache.dubbo.rpc.cluster.router.mesh.rule.destination.Subset;
import org.apache.dubbo.rpc.cluster.router.mesh.rule.virtualservice.DubboMatchRequest;
import org.apache.dubbo.rpc.cluster.router.mesh.rule.virtualservice.DubboRoute;
import org.apache.dubbo.rpc.cluster.router.mesh.rule.virtualservice.DubboRouteDetail;
import org.apache.dubbo.rpc.cluster.router.mesh.rule.virtualservice.VirtualServiceRule;
import org.apache.dubbo.rpc.cluster.router.mesh.rule.virtualservice.VirtualServiceSpec;
import org.apache.dubbo.rpc.cluster.router.mesh.rule.virtualservice.destination.DubboDestination;
import org.apache.dubbo.rpc.cluster.router.mesh.rule.virtualservice.destination.DubboRouteDestination;
import org.apache.dubbo.rpc.cluster.router.mesh.rule.virtualservice.match.StringMatch;
import org.apache.dubbo.rpc.cluster.router.mesh.util.VsDestinationGroupRuleListener;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;


public class MeshRuleRouter implements Router, VsDestinationGroupRuleListener {

    private int priority = -500;
    private boolean force = false;
    private URL url;

    private volatile VsDestinationGroup vsDestinationGroup;

    private Map<String, String> sourcesLabels = new HashMap<>();

    private volatile List<Invoker<?>> invokerList = new ArrayList<>();

    private volatile Map<String, List<Invoker<?>>> subsetMap;

    private volatile String remoteAppName;

    private static final String INVALID_APP_NAME = "unknown";

    public MeshRuleRouter(URL url) {
        this.url = url;
        sourcesLabels.putAll(url.getParameters());
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation) throws RpcException {

        // 挺关键的，先是全根据你的rpc invocation，去获取dubbo route目标地址
        // 就是说，一旦说你要是匹配上了一些规则后，此时就会把你的请求路由到你的目标地址去
        // 这行代码执行完毕，此时就把我们的匹配规则都执行完毕，每个规则匹配上的，就对应了一个目标地址列表
        // 拿到的就是匹配规则的目标地址列表
        List<DubboRouteDestination> routeDestination = getDubboRouteDestination(invocation);

        if (routeDestination == null) {
            return invokers;
        } else {
            // 首先，此时你拿到的目标地址列表，可能是多个
            // 先是随机的拿到一个destination
            DubboRouteDestination dubboRouteDestination = routeDestination.get(ThreadLocalRandom.current().nextInt(routeDestination.size()));
            // 这个直接是一个嵌套获取
            DubboDestination dubboDestination = dubboRouteDestination.getDestination();
            // 再从里面拿到对应的一个subset值
            String subset = dubboDestination.getSubset();

            List<Invoker<?>> result;

            Map<String, List<Invoker<?>>> subsetMapCopy = this.subsetMap;

            //TODO make intersection with invokers
            if (subsetMapCopy != null) {

                do {
                    // 就直接根据你的subset，就获取到了对应的invokers列表
                    result = subsetMapCopy.get(subset);

                    if (CollectionUtils.isNotEmpty(result)) {
                        return (List) result;
                    }

                    // fallback
                    // 在那个上面，如果说根据你的第一优先的subset获取到了invokers，就直接返回了
                    // 否则的话呢，就会走fallback
                    dubboRouteDestination = dubboDestination.getFallback();
                    if (dubboRouteDestination == null) {
                        break;
                    }
                    dubboDestination = dubboRouteDestination.getDestination();

                    subset = dubboDestination.getSubset();
                } while (true);

                return null;
            }
        }

        return invokers;
    }

    @Override
    public <T> void notify(List<Invoker<T>> invokers) {
        List invokerList = invokers == null ? Collections.emptyList() : invokers;
        this.invokerList = invokerList;
        registerAppRule(invokerList);
        computeSubset();
    }


    private void registerAppRule(List<Invoker<?>> invokers) {
        if (StringUtils.isEmpty(remoteAppName)) {
            synchronized (this) {
                if (StringUtils.isEmpty(remoteAppName) && CollectionUtils.isNotEmpty(invokers)) {
                    for (Invoker invoker : invokers) {
                        String applicationName = invoker.getUrl().getRemoteApplication();
                        if (StringUtils.isNotEmpty(applicationName) && !INVALID_APP_NAME.equals(applicationName)) {
                            remoteAppName = applicationName;
                            MeshRuleManager.register(remoteAppName, this);
                            break;
                        }
                    }
                }
            }
        }
    }


    // 如果说你的路由规则最新的变化，反向推送给你，此时就会回调做一个更新
    // 这个东西肯定是从我们的rule路由规则里提取出来的
    @Override
    public void onRuleChange(VsDestinationGroup vsDestinationGroup) {
        this.vsDestinationGroup = vsDestinationGroup;
        computeSubset();
    }

    @Override
    public boolean isRuntime() {
        return true;
    }

    @Override
    public boolean isForce() {
        return force;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    private List<DubboRouteDestination> getDubboRouteDestination(Invocation invocation) {
        // 规则是如何进行解析，有配置变动，反向推送，然后进行解析，以及去重新处理和刷新一个invokers映射关系
        // subset -> invokers是如何进行映射的
        if (vsDestinationGroup != null) {
            // 拿到的是我们的匹配规则
            List<VirtualServiceRule> virtualServiceRuleList = vsDestinationGroup.getVirtualServiceRuleList();
            if (virtualServiceRuleList.size() > 0) {
                for (VirtualServiceRule virtualServiceRule : virtualServiceRuleList) {
                    // 在这里就是说封装了我们的规则匹配 + 目标地址匹配，这两个事情就做完了
                    DubboRoute dubboRoute = getDubboRoute(virtualServiceRule, invocation);
                    if (dubboRoute != null) {
                        return getDubboRouteDestination(dubboRoute, invocation);
                    }
                }
            }
        }
        return null;
    }

    protected DubboRoute getDubboRoute(VirtualServiceRule virtualServiceRule, Invocation invocation) {
        // 你要调用的服务名称
        String serviceName = invocation.getServiceName();

        VirtualServiceSpec spec = virtualServiceRule.getSpec();
        List<DubboRoute> dubboRouteList = spec.getDubbo();
        if (dubboRouteList.size() > 0) {
            for (DubboRoute dubboRoute : dubboRouteList) {
                // 刚开始就是先是拿到services，做一个服务名称的匹配
                List<StringMatch> stringMatchList = dubboRoute.getServices();
                if (CollectionUtils.isEmpty(stringMatchList)) {
                    return dubboRoute;
                }
                for (StringMatch stringMatch : stringMatchList) {
                    if (StringMatch.isMatch(stringMatch, serviceName)) {
                        return dubboRoute;
                    }
                }
            }
        }
        return null;
    }


    protected List<DubboRouteDestination> getDubboRouteDestination(DubboRoute dubboRoute, Invocation invocation) {

        List<DubboRouteDetail> dubboRouteDetailList = dubboRoute.getRoutedetail();
        if (dubboRouteDetailList.size() > 0) {
            // 对每一个dubbo route detail去做一个匹配
            DubboRouteDetail dubboRouteDetail = findMatchDubboRouteDetail(dubboRouteDetailList, invocation);
            if (dubboRouteDetail != null) {
                // 如果说能够匹配上，此时就可以拿到这个detail里面的route
                return dubboRouteDetail.getRoute();
            }
        }

        return null;
    }

    protected DubboRouteDetail findMatchDubboRouteDetail(List<DubboRouteDetail> dubboRouteDetailList, Invocation invocation) {

        // 先从rpc调用里提取出来，方法名称，参数类型，参数值列表
        String methodName = invocation.getMethodName();
        String[] parameterTypeList = invocation.getCompatibleParamSignatures();
        Object[] parameters = invocation.getArguments();


        for (DubboRouteDetail dubboRouteDetail : dubboRouteDetailList) {
            List<DubboMatchRequest> matchRequestList = dubboRouteDetail.getMatch();
            if (CollectionUtils.isEmpty(matchRequestList)) {
                return dubboRouteDetail;
            }

            boolean match = true;

            //FIXME to deal with headers
            for (DubboMatchRequest dubboMatchRequest : matchRequestList) {
                if (!DubboMatchRequest.isMatch(dubboMatchRequest, methodName, parameterTypeList, parameters,
                        sourcesLabels,
                        new HashMap<>(), invocation.getAttachments(),
                        new HashMap<>())) {
                    match = false;
                    break;
                }
            }

            if (match) {
                return dubboRouteDetail;
            }
        }
        return null;
    }


    protected synchronized void computeSubset() {
        if (CollectionUtils.isEmpty(invokerList)) {
            this.subsetMap = null;
            return;
        }

        if (vsDestinationGroup == null) {
            this.subsetMap = null;
            return;
        }

        // 他此时会根据你的配置信息里的subset
        // 此时通过解析和提取，做一个映射，会拿到每个subset的name名称对应的一批invoker机器列表
        // map，name -> invokers
        Map<String, List<Invoker<?>>> subsetMap = computeSubsetMap(invokerList, vsDestinationGroup.getDestinationRuleList());

        if (subsetMap.size() == 0) {
            this.subsetMap = null;
        } else {
            this.subsetMap = subsetMap;
        }
    }


    protected Map<String, List<Invoker<?>>> computeSubsetMap(List<Invoker<?>> invokers, List<DestinationRule> destinationRules) {
        Map<String, List<Invoker<?>>> subsetMap = new HashMap<>();

        // 遍历的是我们的目标地址映射的规则
        for (DestinationRule destinationRule : destinationRules) {
            DestinationRuleSpec destinationRuleSpec = destinationRule.getSpec();
            // 核心的是提取出来你的所有的subset配置
            List<Subset> subsetList = destinationRuleSpec.getSubsets();
            // 遍历你的subset配置
            for (Subset subset : subsetList) {
                String subsetName = subset.getName();
                List<Invoker<?>> subsetInvokerList = new ArrayList<>();
                subsetMap.put(subsetName, subsetInvokerList);

                Map<String, String> labels = subset.getLabels();

                // labels标签和我们的provider invoker实例集群，是如何映射的
                for (Invoker<?> invoker : invokers) {
                    // 每一个目标invoker都有一个url，这个url里面就会包含那个provider所有的信息，包括接口、方法、各种属性和参数
                    Map<String, String> parameters = invoker.getUrl().getServiceParameters(url.getProtocolServiceKey());
                    // 此时就会判断，如果你当前的这个provider invoker，他的各种各样的参数map跟labels标签都匹配上的话，此时就可以算做这个subset里的一台机器
                    if (containMapKeyValue(parameters, labels)) {
                        subsetInvokerList.add(invoker);
                    }
                }
            }
        }

        return subsetMap;
    }


    protected boolean containMapKeyValue(Map<String, String> originMap, Map<String, String> inputMap) {
        if (inputMap == null || inputMap.size() == 0) {
            return true;
        }

        for (Map.Entry<String, String> entry : inputMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            String originMapValue = originMap.get(key);
            if (!value.equals(originMapValue)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public void stop() {
        MeshRuleManager.unregister(this);
    }

    /**
     * just for test
     * @param vsDestinationGroup
     */
    protected void setVsDestinationGroup(VsDestinationGroup vsDestinationGroup) {
        this.vsDestinationGroup = vsDestinationGroup;
    }

    /**
     * just for test
     * @param sourcesLabels
     */
    protected void setSourcesLabels(Map<String, String> sourcesLabels) {
        this.sourcesLabels = sourcesLabels;
    }

    /**
     * just for test
     * @param invokerList
     */
    protected void setInvokerList(List<Invoker<?>> invokerList) {
        this.invokerList = invokerList;
    }

    /**
     * just for test
     * @param subsetMap
     */
    protected void setSubsetMap(Map<String, List<Invoker<?>>> subsetMap) {
        this.subsetMap = subsetMap;
    }


    public VsDestinationGroup getVsDestinationGroup() {
        return vsDestinationGroup;
    }

    public Map<String, String> getSourcesLabels() {
        return sourcesLabels;
    }

    public List<Invoker<?>> getInvokerList() {
        return invokerList;
    }

    public Map<String, List<Invoker<?>>> getSubsetMap() {
        return subsetMap;
    }
}
