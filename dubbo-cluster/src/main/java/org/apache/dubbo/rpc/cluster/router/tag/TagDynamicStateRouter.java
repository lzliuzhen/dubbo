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
package org.apache.dubbo.rpc.cluster.router.tag;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.configcenter.ConfigChangeType;
import org.apache.dubbo.common.config.configcenter.ConfigChangedEvent;
import org.apache.dubbo.common.config.configcenter.ConfigurationListener;
import org.apache.dubbo.common.config.configcenter.DynamicConfiguration;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.RouterChain;
import org.apache.dubbo.rpc.cluster.router.state.AbstractStateRouter;
import org.apache.dubbo.rpc.cluster.router.state.BitList;
import org.apache.dubbo.rpc.cluster.router.state.RouterCache;
import org.apache.dubbo.rpc.cluster.router.tag.model.TagRouterRule;
import org.apache.dubbo.rpc.cluster.router.tag.model.TagRuleParser;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.apache.dubbo.common.constants.CommonConstants.ANYHOST_VALUE;
import static org.apache.dubbo.common.constants.CommonConstants.TAG_KEY;
import static org.apache.dubbo.rpc.Constants.FORCE_USE_TAG;

/**
 * TagDynamicStateRouter, "application.tag-router"
 */
public class TagDynamicStateRouter extends AbstractStateRouter implements ConfigurationListener {
    public static final String NAME = "TAG_ROUTER";
    private static final int TAG_ROUTER_DEFAULT_PRIORITY = 100;
    private static final Logger logger = LoggerFactory.getLogger(TagDynamicStateRouter.class);
    private static final String RULE_SUFFIX = ".tag-router";
    private static final String NO_TAG = "noTag";

    // TagRouterRule
    private TagRouterRule tagRouterRule;
    private String application;

    public TagDynamicStateRouter(URL url, RouterChain chain) {
        super(url, chain);
        this.priority = TAG_ROUTER_DEFAULT_PRIORITY;
    }

    // 在dubbo里面，基于tag标签的动态路由，是很有用的
    // 我们可以去设置标签组，不同的标签组，里面划分了不同的机器
    // tag1里面对应了一部分的机器；tag2里面对应了一部分的机器，可以实现蓝绿部署，灰度发布
    // tag1是蓝组，此时是一批老版本的机器；tag2是红组，是一批新版本的机器
    // tag1是80%的老版本机器；tag2是20%的新版本机器，此时可以约定，某些流量就给tag2就可以了

    @Override
    public synchronized void process(ConfigChangedEvent event) {
        if (logger.isDebugEnabled()) {
            logger.debug("Notification of tag rule, change type is: " + event.getChangeType() + ", raw rule is:\n " +
                event.getContent());
        }

        try {
            if (event.getChangeType().equals(ConfigChangeType.DELETED)) {
                this.tagRouterRule = null;
            } else {
                this.tagRouterRule = TagRuleParser.parse(event.getContent());
            }
        } catch (Exception e) {
            logger.error("Failed to parse the raw tag router rule and it will not take effect, please check if the " +
                "rule matches with the template, the raw rule is:\n ", e);
        }
    }

    @Override
    public <T> BitList<Invoker<T>> route(BitList<Invoker<T>> invokers, RouterCache<T> cache, URL url,
                                         Invocation invocation) throws RpcException {

        // metadata，其实就是对应的tag router rule
        final TagRouterRule tagRouterRuleCopy = (TagRouterRule) cache.getAddrMetadata();
        // 包括此时会从url或者invocation里拿到对应的具体的tag标签的数值
        String tag = StringUtils.isEmpty(invocation.getAttachment(TAG_KEY)) ? url.getParameter(TAG_KEY) :
            invocation.getAttachment(TAG_KEY);
        // key->invokers bitlist的缓存数据
        ConcurrentMap<String, BitList<Invoker<T>>> addrPool = cache.getAddrPool();

        if (StringUtils.isEmpty(tag)) {
            return invokers.intersect(addrPool.get(NO_TAG), invokers.getUnmodifiableList());
        } else {
            // addrPool，key是什么？tag数值，每个tag数值就对应了一批invokers bitlist
            // tag的规则配置，tag1->对应了一批机器，tag2->对应了一批机器，一批机器就是一个invokers bitlist
            // addrPool，tag->invokers bitlist
            BitList<Invoker<T>> result = addrPool.get(tag);

            if (CollectionUtils.isNotEmpty(result) || (tagRouterRuleCopy != null && tagRouterRuleCopy.isForce())
                || isForceUseTag(invocation)) {
                // 传递进来的invokers，会对我们的result和unmodifiable list，做一个交集的处理
                return invokers.intersect(result, invokers.getUnmodifiableList());
            } else {
                invocation.setAttachment(TAG_KEY, NO_TAG);
                return invokers;
            }
        }
    }

    private boolean isForceUseTag(Invocation invocation) {
        return Boolean.parseBoolean(invocation.getAttachment(FORCE_USE_TAG, url.getParameter(FORCE_USE_TAG, "false")));
    }

    @Override
    public boolean isRuntime() {
        return tagRouterRule != null && tagRouterRule.isRuntime();
    }

    @Override
    public boolean isEnable() {
        return tagRouterRule != null && tagRouterRule.isEnabled();
    }

    @Override
    public boolean isForce() {
        return tagRouterRule != null && tagRouterRule.isForce();
    }

    @Override
    public String getName() {
        return "TagDynamic";
    }

    @Override
    public boolean shouldRePool() {
        return false;
    }

    @Override
    public <T> RouterCache<T> pool(List<Invoker<T>> invokers) {

        RouterCache<T> routerCache = new RouterCache<>();

        // 每个state router的缓存，都是这一份东西，tag -> invokers bitlist
        ConcurrentHashMap<String, BitList<Invoker<T>>> addrPool = new ConcurrentHashMap<>();

        final TagRouterRule tagRouterRuleCopy = tagRouterRule;


        if (tagRouterRuleCopy == null || !tagRouterRuleCopy.isValid() || !tagRouterRuleCopy.isEnabled()) {
            BitList<Invoker<T>> noTagList = new BitList<>(invokers, false);
            addrPool.put(NO_TAG, noTagList);
            routerCache.setAddrPool(addrPool);
            return routerCache;
        }

        // 根据tag规则 ，会拿到一份tags list
        List<String> tagNames = tagRouterRuleCopy.getTagNames();
        Map<String, List<String>> tagnameToAddresses = tagRouterRuleCopy.getTagnameToAddresses();

        for (String tag : tagNames) {
            // 对于每个tag标签，都会去获取到这个tag标签对应的机器地址的list
            List<String> addresses = tagnameToAddresses.get(tag);
            // 目标invokers会封装为一个bitlist
            BitList<Invoker<T>> list = new BitList<>(invokers, true);

            if (CollectionUtils.isEmpty(addresses)) {
                list.addAll(invokers);
            } else {
                for (int index = 0; index < invokers.size(); index++) {
                    Invoker<T> invoker = invokers.get(index);
                    // 对于这个invoker和tag标签对应的地址列表做一个匹配
                    if (addressMatches(invoker.getUrl(), addresses)) {
                        // 如果说是在的话，此时就可以把invokers index，加入到bitlist里面的bitmap里面去
                        list.addIndex(index);
                    }
                }
            }

            // 每个tag -> bitlist（invokers list，真正机器的invoker index -> bitmap）
            addrPool.put(tag, list);
        }

        List<String> addresses = tagRouterRuleCopy.getAddresses();
        BitList<Invoker<T>> noTagList = new BitList<>(invokers, true);

        for (int index = 0; index < invokers.size(); index++) {
            Invoker<T> invoker = invokers.get(index);
            if (addressNotMatches(invoker.getUrl(), addresses)) {
                noTagList.addIndex(index);
            }
        }
        addrPool.put(NO_TAG, noTagList);

        // addr pool, rule规则
        routerCache.setAddrPool(addrPool);
        routerCache.setAddrMetadata(tagRouterRuleCopy);

        return routerCache;
    }

    private boolean addressMatches(URL url, List<String> addresses) {
        return addresses != null && checkAddressMatch(addresses, url.getHost(), url.getPort());
    }

    private boolean addressNotMatches(URL url, List<String> addresses) {
        return addresses == null || !checkAddressMatch(addresses, url.getHost(), url.getPort());
    }

    private boolean checkAddressMatch(List<String> addresses, String host, int port) {
        for (String address : addresses) {
            try {
                if (NetUtils.matchIpExpression(address, host, port)) {
                    return true;
                }
                if ((ANYHOST_VALUE + ":" + port).equals(address)) {
                    return true;
                }
            } catch (Exception e) {
                logger.error("The format of ip address is invalid in tag route. Address :" + address, e);
            }
        }
        return false;
    }

    public void setApplication(String app) {
        this.application = app;
    }

    @Override
    public <T> void notify(List<Invoker<T>> invokers) {
        if (CollectionUtils.isEmpty(invokers)) {
            return;
        }

        Invoker<T> invoker = invokers.get(0);
        URL url = invoker.getUrl();
        String providerApplication = url.getRemoteApplication();

        if (StringUtils.isEmpty(providerApplication)) {
            logger.error("TagRouter must getConfig from or subscribe to a specific application, but the application " +
                "in this TagRouter is not specified.");
            return;
        }

        synchronized (this) {
            if (!providerApplication.equals(application)) {
                if (StringUtils.isNotEmpty(application)) {
                    ruleRepository.removeListener(application + RULE_SUFFIX, this);
                }
                // 这里会拼接出来一个key，目标对应的provider集群，app的名称，rule后缀
                String key = providerApplication + RULE_SUFFIX;
                ruleRepository.addListener(key, this); // 把自己作为一个监听加入进去就可以了
                // 后续如果说有一个规则的变化的话，此时会通过process方法回调通知他，他重新解析拿到最新的tag规则就可以了
                application = providerApplication;
                String rawRule = ruleRepository.getRule(key, DynamicConfiguration.DEFAULT_GROUP);
                if (StringUtils.isNotEmpty(rawRule)) {
                    // 他也可以主动拿到最新的rule，自己调用自己的process就可以了
                    this.process(new ConfigChangedEvent(key, DynamicConfiguration.DEFAULT_GROUP, rawRule));
                }
            }
        }
    }

    @Override
    public void stop() {
        if (StringUtils.isNotEmpty(application)) {
            ruleRepository.removeListener(application + RULE_SUFFIX, this);
        }
    }
}
