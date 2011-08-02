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

import com.cloudera.flume.agent.FlumeNode;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.google.common.base.Preconditions;

/**
 * This decorator adds a the capabilty to Throttle the data going out of the
 * sink. Each Chokedecorator is associated with a chokeId, and all the
 * choke-decorators with the same chokeId are throttled together with some max
 * data transfer limit. The mapping from the chokeId to limit is set by the
 * Master and passed to FlumeNodes using an RPC call called getChokeMap().
 */

public class ChokeDecorator<S extends EventSink> extends EventSinkDecorator<S> {

  // this is the throttling limit set in KB/sec.
  final String chokeId;
  private ChokeManager chokeMan;

  public ChokeDecorator(S s, String tId) {
    super(s);
    chokeId = tId;
  }

  /**
   * This append can block for a little while if the number of bytes shipped
   * accross this Choke has reached its limit. But it does not block forever.
   */
  @Override
  public void append(Event e) throws IOException, InterruptedException {
    chokeMan.spendTokens(chokeId, e.getBody().length);
    super.append(e);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void open() throws IOException, InterruptedException {
    this.chokeMan = FlumeNode.getInstance().getChokeManager();
    super.open();
  }

  /**
   * Returns the ChokeId corresponding to this choke.
   */
  public String getChokeId() {
    return chokeId;

  }

  public static SinkDecoBuilder builder() {

    return new SinkDecoBuilder() {
      // In the current version we don't check if the id (argv[0]) is valid
      // chokeId.
      // If it is not, then this this choke decorator will have no throttling
      // limit.
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 1,
            "usage: choke(\"chokeId\")");
        String chokeID = argv[0];
        return new ChokeDecorator<EventSink>(null, chokeID);
      }
    };
  }

}
