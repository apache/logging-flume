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

import java.util.Date;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.core.Event;
import com.cloudera.util.Clock;

/**
 * Triggers when a specified duration of time has passed.
 */
public class TimeTrigger implements RollTrigger {
  Tagger tagger;
  long maxAge;

  public TimeTrigger() {
    FlumeConfiguration conf = FlumeConfiguration.get();
    this.maxAge = conf.getAgentLogMaxAge();
    this.tagger = new ProcessTagger();
  }

  public TimeTrigger(FlumeConfiguration conf, Tagger t) {
    this.maxAge = conf.getAgentLogMaxAge();
    this.tagger = t;
  }

  public TimeTrigger(long maxAge) {
    this.maxAge = maxAge;
    this.tagger = new ProcessTagger();
  }

  public TimeTrigger(Tagger t, long maxAge) {
    this.maxAge = maxAge;
    this.tagger = t;
  }

  @Override
  public boolean isTriggered() {
    Date d = tagger.getDate();
    long time = Clock.unixTime(); // this will kill performance.
    long delta = time - d.getTime();
    return (delta >= maxAge);
  }

  @Override
  public void append(Event e) {
    // DO NOTHING
  }

  @Override
  public Tagger getTagger() {
    return tagger;
  }

  @Override
  public void reset() {
    // TODO (jon) make this cleaner
    tagger.newTag(); // this updates the timestamp in the tagger
  }

}
