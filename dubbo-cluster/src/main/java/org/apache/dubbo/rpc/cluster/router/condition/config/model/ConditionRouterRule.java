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
package org.apache.dubbo.rpc.cluster.router.condition.config.model;

import org.apache.dubbo.rpc.cluster.router.AbstractRouterRule;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.dubbo.rpc.cluster.Constants.CONDITIONS_KEY;

/**
 *
 */
public class ConditionRouterRule extends AbstractRouterRule {

    private List<String> conditions; // 包含了一个字符串的list
    // 每个string就是一个condition条件规则，这个string的字符串条件规则，在这里就是我刚才写的那种规则
    // host =xx => host
    // dubbo，条件规则的语法，上网去查一下就可以了


    @SuppressWarnings("unchecked")
    public static ConditionRouterRule parseFromMap(Map<String, Object> map) {
        ConditionRouterRule conditionRouterRule = new ConditionRouterRule();
        conditionRouterRule.parseFromMap0(map);

        Object conditions = map.get(CONDITIONS_KEY);
        if (conditions != null && List.class.isAssignableFrom(conditions.getClass())) {
            conditionRouterRule.setConditions(((List<Object>) conditions).stream()
                    .map(String::valueOf).collect(Collectors.toList()));
        }

        return conditionRouterRule;
    }

    public ConditionRouterRule() {
    }

    public List<String> getConditions() {
        return conditions;
    }

    public void setConditions(List<String> conditions) {
        this.conditions = conditions;
    }
}
