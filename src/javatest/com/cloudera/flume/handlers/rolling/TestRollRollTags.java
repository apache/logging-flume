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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.MaskDecorator;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.endtoend.ValueDecorator;
import com.cloudera.util.BenchmarkHarness;

/**
 * This test case demonstrates a usecase where a tag conflict occurs.
 */
public class TestRollRollTags {

  @Test(expected = IllegalArgumentException.class)
  public void testTagConflict() {
    Event e = new EventImpl("foo".getBytes());
    Attributes.setString(e, "duped", "first");
    Attributes.setString(e, "duped", "second");
  }

  @Test
  public void testMaskNoConflict() throws IOException {
    MemorySinkSource mem = new MemorySinkSource();
    EventSink s1 = new ValueDecorator<EventSink>(mem, "duped", "second"
        .getBytes());
    EventSink s2 = new MaskDecorator<EventSink>(s1, "duped");
    EventSink snk = new ValueDecorator<EventSink>(s2, "duped", "first"
        .getBytes());
    snk.open();

    Event e = new EventImpl("foo".getBytes());
    snk.append(e);
    snk.close();

    Event e2 = mem.next();
    assertEquals("second", Attributes.readString(e2, "duped"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRollRollConflict() throws IOException, FlumeSpecException {
    EventSink snk = new CompositeSink(new Context(),
        "{value(\"rolltag\",\"foofoo\") =>   roll(10000) {null} } ");
    Event e = new EventImpl("foo".getBytes());
    snk.open();
    snk.append(e); // should bork.
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRollRollBork() throws IOException, FlumeSpecException {
    EventSink snk = new CompositeSink(new Context(),
        "roll(10000) { roll(10000) { null } } ");
    Event e = new EventImpl("foo".getBytes());
    snk.open();
    snk.append(e); // should bork.
  }

  @Test
  public void testRollRollNoConflict() throws IOException, FlumeSpecException {
    EventSink snk = new CompositeSink(new Context(),
        "{value(\"rolltag\",\"foofoo\") =>  "
            + "{ mask(\"rolltag\")=>  roll(10000) { null}}} ");

    Event e = new EventImpl("foo".getBytes());
    snk.open();
    snk.append(e); // should not bork.
  }

  @Test
  public void testRollRollNoBork() throws IOException, FlumeSpecException {
    EventSink snk = new CompositeSink(new Context(),
        "roll(10000) {{ mask(\"rolltag\") => roll(10000) {null} }} ");
    Event e = new EventImpl("foo".getBytes());
    snk.open();
    snk.append(e); // should not bork.
  }

  // This used to trigger an exception but now that the rolltag is suppressed it
  // should no longer.
  @Test
  public void testAgentCollector() throws FlumeSpecException, IOException {
    BenchmarkHarness.setupLocalWriteDir();
    File path = File.createTempFile("collector", ".tmp");
    path.deleteOnExit();

    EventSink snk = new CompositeSink(new Context(),
        "{ ackedWriteAhead => roll(1000) { dfs(\"file://" + path + "\") } }");
    Event e = new EventImpl("foo".getBytes());
    snk.open();
    snk.append(e); // should not bork.
    snk.close();
    BenchmarkHarness.cleanupLocalWriteDir();
  }

  @Test
  public void testAgentCollectorFixed() throws FlumeSpecException, IOException {
    BenchmarkHarness.setupLocalWriteDir();
    File path = File.createTempFile("collector", ".tmp");
    path.deleteOnExit();

    EventSink snk = new CompositeSink(new Context(),
        "{ ackedWriteAhead => { mask(\"rolltag\") => roll(1000) { dfs(\"file://"
            + path + "\") } } }");
    Event e = new EventImpl("foo".getBytes());
    snk.open();
    snk.append(e); // should not bork.
    snk.close();
    BenchmarkHarness.cleanupLocalWriteDir();
  }
}
