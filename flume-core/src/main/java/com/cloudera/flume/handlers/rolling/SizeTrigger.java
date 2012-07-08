/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.handlers.rolling;

import com.cloudera.flume.core.Event;

/**
 * Triggers when the size of the body of messages reaches some threshold
 */
public class SizeTrigger implements RollTrigger {
  final long maxSize;

  long size;
  Tagger tagger;

  public SizeTrigger(long maxSize, Tagger t) {
    this.maxSize = maxSize;
    this.tagger = t;
  }

  @Override
  public void append(Event e) {
    size += e.getBody().length;
  }

  @Override
  public boolean isTriggered() {
    return maxSize <= size;
  }

  @Override
  public Tagger getTagger() {
    return tagger;
  }

  @Override
  public void reset() {
    size = 0;
  }

}
