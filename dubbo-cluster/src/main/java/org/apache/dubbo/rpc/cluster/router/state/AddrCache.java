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
package org.apache.dubbo.rpc.cluster.router.state;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.dubbo.rpc.Invoker;

/***
 * Address cache,
 * used to cache the results of the StaterRouter's asynchronous address list calculations.
 * @param <T>
 * @since 3.0
 */
public class AddrCache<T> {

    // addr地址缓存数据，缓存了什么，invokers列表，有一个router cache的map
    private List<Invoker<T>> invokers;
    // key->router cache
    private Map<String, RouterCache<T>> cache = Collections.emptyMap();

    public List<Invoker<T>> getInvokers() {
        return invokers;
    }

    public void setInvokers(List<Invoker<T>> invokers) {
        this.invokers = invokers;
    }

    public Map<String, RouterCache<T>> getCache() {
        return cache;
    }

    public void setCache(Map<String, RouterCache<T>> cache) {
        this.cache = cache;
    }
}
