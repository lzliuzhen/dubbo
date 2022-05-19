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
package org.apache.dubbo.common.threadpool.manager;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

// ring，环的概念，dubbo自己实现了一个环的数据结构
public class Ring<T> {

    AtomicInteger count = new AtomicInteger();

    // 这个环数据结构，底层是基于array list来实现的，为什么叫做环
    // 就是你可以不停的往里面插入数据这样子，到尾部了以后，可以从头开始继续插入，环形数据结构，重复使用
    private List<T> itemList = new CopyOnWriteArrayList<T>();

    public void addItem(T t) {
        if (t != null) {
            itemList.add(t);
        }
    }

    public T pollItem() {
        if (itemList.isEmpty()) {
            return null;
        }
        if (itemList.size() == 1) {
            return itemList.get(0);
        }

        if (count.intValue() > Integer.MAX_VALUE - 10000) {
            count.set(count.get() % itemList.size()); // count值太大了，此时对count值做一个重置就可以了
            // 还有1w条数据，就达到了Integer.MAX_VALUE值了
            // 此时就开始用环的概念，重复循环使用你的count就可以了
        }

        // count指的这个意思，是我们的poll次数
        // 每次进行poll，你的count就会进行累加，但是每次累加后都是对你的list size做一个取模
        // list里面总共就8个东西，每次poll都进行count累加的过程，此时就会第一次拿到第一个东西，第二次是第二个东西，一直到第8个东西
        // 取模，又会回到第一个东西来进行poll，ring，并不是我刚才所说的，放数据的时候当成数据环来重复的使用和放入
        // list里面的数据一般来说可能不多，ring，环，不停的循环的取用你的list里面的东西来使用，循环
        int index = Math.abs(count.getAndIncrement()) % itemList.size();
        return itemList.get(index);
    }

    public T peekItem() {
        if (itemList.isEmpty()) {
            return null;
        }
        if (itemList.size() == 1) {
            return itemList.get(0);
        }
        int index = Math.abs(count.get()) % itemList.size();
        return itemList.get(index);
    }

    public List<T> listItems() {
        return Collections.unmodifiableList(itemList);
    }
}
