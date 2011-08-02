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
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;

/**
 * This is for throughput testing. Here we put a set of events into memory and
 * then stream them through the system at max rate.
 * 
 * It is both a sink and a source. The idea is to use it as a sink where
 * everything is streamed in and then use it as a source for some downstream
 * sink.
 * 
 * This will let us determine what network and disk bandwidth limiters are.
 */
public class MemorySinkSource extends EventSink.Base implements EventSource {

  final static Logger LOG = Logger.getLogger(MemorySinkSource.class);

  List<Event> evts = new ArrayList<Event>();
  int idx = 0;

  @Override
  public void append(Event e) throws IOException {
    evts.add(e);
    super.append(e);
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing mem source sink that has " + evts.size() + " elements");
  }

  @Override
  public void open() throws IOException {
    LOG.info("Opening mem source sink that has with " + evts.size()
        + " elements");
    idx = 0;
  }

  @Override
  public Event next() throws IOException {
    if (idx == evts.size())
      return null;

    Event e = evts.get(idx);
    idx++;
    // TODO missing source reports
    return e;
  }

  /**
   * Create a memory based source of count events. Each event has s with count
   * appended as its body.
   */
  public static MemorySinkSource cannedData(String s, int count)
      throws IOException {
    MemorySinkSource mss = new MemorySinkSource();
    for (int i = 0; i < count; i++) {
      Event e = new EventImpl((s + " " + count).getBytes());
      mss.append(e);
    }

    return mss;
  }

  public void reset() {
    evts.clear();
    idx = 0;
  }

  /**
   * This takes a source, drains its data to memory, and then returns it as
   * source that is ready to be read from. This method assumes that the source
   * is finite, and will fit into memory.
   */
  public static MemorySinkSource bufferize(EventSource src) throws IOException {
    MemorySinkSource mem = new MemorySinkSource();
    src.open();
    Event e;
    while ((e = src.next()) != null) {
      mem.append(e);
    }
    src.close();
    return mem;
  }
}
