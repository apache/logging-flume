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
package com.cloudera.flume.handlers.debug;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.google.common.base.Preconditions;

/**
 * This decorator adds a latch hook that will block the sink until the latch is
 * triggered. Again this is for allowing different error conditions to be
 * triggered.
 * 
 * Allow precount events to pass first.
 * 
 * Then latch until trigger is called the specified number of times.
 * 
 * Afterwards, just work.
 * 
 * TODO (jon) this isn't completely thought out.
 */
public class LatchedDecorator<S extends EventSink> extends
    EventSinkDecorator<S> {

  final CountDownLatch latch;
  final int pre;
  final AtomicInteger precount;

  public LatchedDecorator(S s, int pre, int count) {
    super(s);
    this.latch = new CountDownLatch(count);
    this.pre = pre;
    this.precount = new AtomicInteger(pre);
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    if (precount.get() > 0) {

      precount.decrementAndGet();
    } else {
      try {
        latch.await();
      } catch (InterruptedException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
    }
    super.append(e);
  }

  /**
   * This decrements the latch counter the impede's progress
   */
  public void trigger() {
    latch.countDown();
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length <= 2,
            "usage: latch([init=1,[pre=0]])");
        int mult = 1;
        int pre = 0;
        if (argv.length >= 1)
          mult = Integer.parseInt(argv[0]);
        if (argv.length >= 2)
          pre = Integer.parseInt(argv[1]);
        return new LatchedDecorator<EventSink>(null, pre, mult);
      }

    };
  }

}
