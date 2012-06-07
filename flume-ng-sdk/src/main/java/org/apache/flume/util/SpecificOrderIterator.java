/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.util;

import java.util.Iterator;
import java.util.List;

/**
 * A utility class that iterates over the given ordered list of items via
 * the specified order array. The entries of the order array indicate the
 * index within the ordered list of items that needs to be picked over the
 * course of iteration.
 */
public class SpecificOrderIterator<T> implements Iterator<T> {

  private final int[] order;
  private final List<T> items;
  private int index = 0;

  public SpecificOrderIterator(int[] orderArray, List<T> itemList) {
    order = orderArray;
    items = itemList;
  }

  @Override
  public boolean hasNext() {
    return index < order.length;
  }

  @Override
  public T next() {
    return items.get(order[index++]);
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
