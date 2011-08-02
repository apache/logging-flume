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
package com.cloudera.flume.handlers.batch;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.handlers.hdfs.WriteableEvent;
import com.google.common.base.Preconditions;

/**
 * This is a pass through that will unbatch batched events or just pass other
 * events through to the decorated sink.
 */
public class UnbatchingDecorator<S extends EventSink> extends
    EventSinkDecorator<S> {

  public UnbatchingDecorator(S s) {
    super(s);
  }

  /**
   * if it is not a batch event, pass it through, otherwise, unbatch and pass
   * through the events.
   */
  @Override
  public void append(Event e) throws IOException {
    if (!BatchingDecorator.isBatch(e)) {
      super.append(e);
      return;
    }

    int sz = ByteBuffer.wrap(e.get(BatchingDecorator.BATCH_SIZE)).getInt();
    byte[] data = e.get(BatchingDecorator.BATCH_DATA);
    DataInput in = new DataInputStream(new ByteArrayInputStream(data));
    for (int i = 0; i < sz; i++) {
      WriteableEvent we = new WriteableEvent();
      we.readFields(in);
      super.append(we);
    }
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 0, "usage: unbatch");
        return new UnbatchingDecorator<EventSink>(null);
      }
    };
  }
}
