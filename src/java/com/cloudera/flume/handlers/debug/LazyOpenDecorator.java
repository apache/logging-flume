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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.google.common.base.Preconditions;

/**
 * This delays turns a sink into a lazy one by delaying opening a node until an
 * append attempt is made.
 * 
 * This is thread safe.
 */
public class LazyOpenDecorator<S extends EventSink> extends
    EventSinkDecorator<S> {
  public static final Logger LOG = LoggerFactory
      .getLogger(LazyOpenDecorator.class);

  /**
   * open has been called on this sink
   */
  boolean logicallyOpen = false;

  /**
   * open has been called on subsink
   */
  boolean actuallyOpen = false;

  public LazyOpenDecorator(S s) {
    super(s);
  }

  @Override
  public void open() {
    Preconditions.checkState(!logicallyOpen,
        "Attempting to open already open LazyOpenDeco");
    logicallyOpen = true;
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    if (logicallyOpen && !actuallyOpen) {
      super.open();
      actuallyOpen = true;
    }

    super.append(e);
  }

  @Override
  public void close() throws IOException, InterruptedException {
    if (actuallyOpen) {
      super.close();
    }

    if (!logicallyOpen) {
      LOG.warn("Closing a lazy sink that was not logically opened");
    }

    actuallyOpen = false;
    logicallyOpen = false;
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 0);
        return new LazyOpenDecorator<EventSink>(null);
      }
    };
  }

}
