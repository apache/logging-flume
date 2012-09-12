/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.util;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * An implementation of OrderSelector which returns objects in random order.
 * Also supports backoff.
 */
public class RandomOrderSelector<T> extends OrderSelector<T> {

  private Random random = new Random(System.currentTimeMillis());

  public RandomOrderSelector(boolean shouldBackOff) {
    super(shouldBackOff);
  }

  @Override
  public synchronized Iterator<T> createIterator() {
    List<Integer> indexList = getIndexList();

    int size = indexList.size();
    int[] indexOrder = new int[size];

    while (indexList.size() != 1) {
      int pick = random.nextInt(indexList.size());
      indexOrder[indexList.size() - 1] = indexList.remove(pick);
    }

    indexOrder[0] = indexList.get(0);

    return new SpecificOrderIterator<T>(indexOrder, getObjects());
  }
}
