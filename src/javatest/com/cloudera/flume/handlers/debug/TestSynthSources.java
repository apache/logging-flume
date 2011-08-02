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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.Test;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.avro.AvroJsonOutputFormat;
import com.cloudera.util.BenchmarkHarness;

/**
 * Test cases for synthetic sources.
 */
public class TestSynthSources {
  final static Logger LOG = Logger.getLogger(TestSynthSources.class.getName());

  /**
   * Test the body generating source
   */
  @Test
  public void checkSynth() throws IOException {
    EventSource src = new SynthSource(5, 10, 1337);
    Event e = null;
    EventSink snk = new ConsoleEventSink(new AvroJsonOutputFormat());
    MemorySinkSource mem = new MemorySinkSource();
    while ((e = src.next()) != null) {
      snk.append(e); // visual inspection
      mem.append(e); // testing
    }

    mem.open();
    int i = 0;
    while ((e = mem.next()) != null) {
      i++;
      assertEquals(10, e.getBody().length);
    }
    assertEquals(5, i);

  }

  /**
   * This makes sure that the synth source is reopened, it will essentially
   * generate the same output. (time stamp and machine may differ)
   */
  @Test
  public void testMultipleVaryMessageBytes() throws IOException {
    Event e1, e2;
    for (EventSource src : BenchmarkHarness.varyMsgBytes.values()) {
      src.open();
      e1 = src.next();
      src.open();
      e2 = src.next();
      assertTrue(Arrays.equals(e1.getBody(), e2.getBody()));
    }
  }

  /**
   * Tests to make sure we get events with the specified number of attributes,
   * with specified attribute size, and values of specified size.
   */
  @Test
  public void checkAttrSynth() throws IOException {
    EventSource src = new AttrSynthSource(5, 10, 20, 15, 1337);
    Event e = null;
    EventSink snk = new ConsoleEventSink(new AvroJsonOutputFormat());
    MemorySinkSource mem = new MemorySinkSource();
    while ((e = src.next()) != null) {
      snk.append(e); // visual inspection
      mem.append(e); // testing
    }

    mem.open();
    int i = 0;
    while ((e = mem.next()) != null) {
      i++;
      Map<String, byte[]> ents = e.getAttrs();
      assertEquals(10, ents.size()); // 10 generated + 1 (service)
      for (String a : ents.keySet()) {
        assertEquals(20, a.length());
      }
      for (byte[] v : ents.values()) {
        assertEquals(15, v.length);
      }
    }
    assertEquals(5, i);
  }

  /**
   * This makes sure that the synth source is reopened, it will essentially
   * generate the same output. (time stamp and machine may differ)
   */
  @Test
  public void testAttrsMultipleVaryMessageBytes() throws IOException {
    Event e1, e2;
    for (EventSource src : BenchmarkHarness.varyNumAttrs.values()) {
      src.open();
      e1 = src.next();
      src.open();
      e2 = src.next();
      assertTrue(Arrays.equals(e1.getBody(), e2.getBody()));
    }
  }

}
