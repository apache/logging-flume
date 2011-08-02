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
package com.cloudera.flume.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import com.cloudera.flume.handlers.debug.NullSink;
import com.cloudera.util.MultipleIOException;
import com.google.common.base.Preconditions;

/**
 * This takes events and splits them based on the value of an attribute
 */
public class DemuxSink<S extends EventSink> extends EventSink.Base {

  String field;
  Map<byte[], S> split;
  EventSink fallthrough;

  public DemuxSink(String field, Map<byte[], S> split, EventSink fallthrough) {
    Preconditions.checkNotNull(field);
    Preconditions.checkNotNull(split);
    this.field = field;
    this.split = split;
    this.fallthrough = fallthrough;
  }

  public DemuxSink(String field, Map<byte[], S> split) {
    this(field, split, new NullSink());
  }

  @Override
  public void append(Event e) throws IOException {
    byte[] val = e.get(field);
    S handler = split.get(val);

    if (handler == null) {
      fallThrough(val, e);
      return;
    }

    handler.append(e);
  }

  public void fallThrough(byte[] val, Event e) throws IOException {
    // default is pass to fallthrough sink
    fallthrough.append(e);
  }

  @Override
  public void close() throws IOException {
    List<IOException> exs = new ArrayList<IOException>();

    for (S snk : split.values()) {
      try {
        snk.close();
      } catch (IOException ioe) {
        exs.add(ioe);
      }
    }

    if (!exs.isEmpty()) {
      throw MultipleIOException.createIOException(exs);
    }
  }

  @Override
  public void open() throws IOException {
    List<IOException> exs = new ArrayList<IOException>();

    for (S snk : split.values()) {
      try {
        snk.open();
      } catch (IOException ioe) {
        exs.add(ioe);
      }
    }

    if (!exs.isEmpty()) {
      throw MultipleIOException.createIOException(exs);
    }
  }

}
