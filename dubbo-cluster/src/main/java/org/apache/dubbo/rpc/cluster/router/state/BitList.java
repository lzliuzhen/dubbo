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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import org.roaringbitmap.IntIterator;
import org.roaringbitmap.RoaringBitmap;

/**
 * BitList based on BitMap implementation.
 *
 * bit list，dubbo自己封装的一个list，这个list的话是实现了jdk里面的List接口
 * 所以说，你可以把他当做是一个正常的list来使用，只不过list里面的一些实现跟jdk提供的不大一样而已
 * arraylist，底层是基于数组实现的一个list，linkedlist基于链表实现的一个list
 * dubbo bitlist，底层肯定是基于bit位相关的东西来实现的一个list
 *
 * @param <E>
 * @since 3.0
 */
public class BitList<E> implements List<E> {
    // RoaringBitmap是一个第三方框架了，没办法进去继续读他的源码了，bit位map数据结构
    private final RoaringBitmap rootMap;
    private final List<E> unmodifiableList;

    // 对于这个bit list没法在初始化后再往里面增加数据
    // 构建的时候，直接放进去一个普通的list，使用里面的数据进行一个初始化，普通的list是不能修改的
    public BitList(List<E> unmodifiableList, boolean empty) {
        this.unmodifiableList = unmodifiableList;
        this.rootMap = new RoaringBitmap(); // bitmap，顾名思义，底层是基于bitmap来实现的，bitlist
        // 对于这个list后续主要是做一些读操作
        if (!empty) {
            this.rootMap.add(0L, unmodifiableList.size());
        }
    }

    public BitList(List<E> unmodifiableList, RoaringBitmap rootMap) {
        this.unmodifiableList = unmodifiableList;
        this.rootMap = rootMap;
    }

    public BitList(List<E> unmodifiableList) {
        this(unmodifiableList, false);
    }

    public List<E> getUnmodifiableList() {
        return unmodifiableList;
    }

    public void addIndex(int index) {
        this.rootMap.add(index);
    }

    public BitList<E> intersect(List<E> b, List<E> totalList) {
        RoaringBitmap resultMap = rootMap.clone();
        resultMap.and(((BitList)b).rootMap);
        return new BitList<>(totalList, resultMap); // bitmap，核心是放了一个映射数据关系
        // index，index -> bitmap -> real index -> total list -> 拿到具体的数值
    }

    @Override
    public int size() {
        return rootMap.getCardinality();
    }

    @Override
    public boolean isEmpty() {
        return rootMap.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        int idx = unmodifiableList.indexOf(o);
        return idx >= 0 && rootMap.contains(idx);
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<E>() {
            private int prev = -1;

            @Override
            public boolean hasNext() {
                return -1 != rootMap.nextValue(prev + 1);
            }

            @Override
            public E next() {
                prev = (int)rootMap.nextValue(prev + 1);
                return unmodifiableList.get(prev);
            }

            @Override
            public void remove() {
                rootMap.remove(prev);
            }
        };
    }

    @Override
    public Object[] toArray() {
        int size = size();
        Object[] obj = new Object[size];
        for (int i = 0; i < size; i++) {
            obj[i] = unmodifiableList.get(rootMap.select(i));
        }
        return obj;
    }

    @Override
    public <T> T[] toArray(T[] a) {
        int size = size();
        Object[] arr = toArray();
        if (a.length < size)
        // Make a new array of a's runtime type, but my contents:
        { return (T[])Arrays.copyOf(arr, size, a.getClass()); }
        System.arraycopy(arr, 0, a, 0, size);
        if (a.length > size) { a[size] = null; }
        return null;
    }

    @Override
    public boolean add(E e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        int idx = unmodifiableList.indexOf(o);
        if (idx > -1) {
            rootMap.remove(idx);
        }
        return true;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(int index, Collection<? extends E> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        for (Object o : c) {
            remove(o);
        }
        return true;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;
    }

    @Override
    public void clear() {
        rootMap.clear();
    }

    @Override
    public E get(int index) {
        int real = rootMap.select(index);
        return unmodifiableList.get(real);
    }

    @Override
    public E set(int index, E element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void add(int index, E element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public E remove(int index) {
        rootMap.remove(index);
        return null;
    }

    @Override
    public int indexOf(Object o) {
        IntIterator intIterator = rootMap.getIntIterator();
        int st = 0;
        while (intIterator.hasNext()) {
            int idxInMap = intIterator.next();
            if (unmodifiableList.get(idxInMap).equals(o)) {
                return st;
            }
            st++;
        }
        return -1;
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<E> listIterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListIterator<E> listIterator(int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<E> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException();
    }

}
