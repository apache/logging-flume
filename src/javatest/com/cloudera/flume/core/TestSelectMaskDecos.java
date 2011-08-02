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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.handlers.debug.MemorySinkSource;

/**
 * Testing selection and masking decorator operations.
 */
public class TestSelectMaskDecos {

  @Test
  public void testSelect() throws IOException {
    MemorySinkSource mem = new MemorySinkSource();
    EventSink sel = new SelectDecorator<EventSink>(mem, "foo", "bar", "bork");

    Event e = new EventImpl("content".getBytes());
    Attributes.setString(e, "foo", "foo data");
    Attributes.setString(e, "bar", "bar data");
    Attributes.setString(e, "baz", "baz data");

    sel.open();
    sel.append(e);
    sel.close();

    mem.open();
    Event e2 = mem.next();
    assertEquals("foo data", Attributes.readString(e2, "foo"));
    assertEquals("bar data", Attributes.readString(e2, "bar"));
    assertEquals(null, Attributes.readString(e2, "baz")); // not selected
    assertEquals(null, Attributes.readString(e2, "bork")); // not present
  }

  /**
   * Successful if no exceptions thrown.
   */
  @Test
  public void testSelectBuilder() throws IOException, FlumeSpecException {
    EventSink snk =
        new CompositeSink(new Context(),
            "{ select(\"foo\", \"bar\",\"bork\") => counter(\"count\") }");
    snk.open();
    Event e = new EventImpl("content".getBytes());
    Attributes.setString(e, "foo", "foo data");
    Attributes.setString(e, "bar", "bar data");
    Attributes.setString(e, "baz", "baz data");
    snk.append(e);
    snk.close();
  }

  @Test
  public void testMask() throws IOException {
    MemorySinkSource mem = new MemorySinkSource();
    EventSink mask = new MaskDecorator<EventSink>(mem, "foo", "bar", "bork");

    Event e = new EventImpl("content".getBytes());
    Attributes.setString(e, "foo", "foo data");
    Attributes.setString(e, "bar", "bar data");
    Attributes.setString(e, "baz", "baz data");

    mask.open();
    mask.append(e);
    mask.close();

    mem.open();
    Event e2 = mem.next();
    assertEquals(null, Attributes.readString(e2, "foo"));
    assertEquals(null, Attributes.readString(e2, "bar"));
    assertEquals("baz data", Attributes.readString(e2, "baz")); // not masked
    assertEquals(null, Attributes.readString(e2, "bork")); // not present
  }

  /**
   * Successful if no exceptions thrown.
   */
  @Test
  public void testMaskBuilder() throws IOException, FlumeSpecException {
    EventSink snk =
        new CompositeSink(new Context(),
            "{ mask(\"foo\", \"bar\",\"bork\") => counter(\"count\") }");
    snk.open();
    Event e = new EventImpl("content".getBytes());
    Attributes.setString(e, "foo", "foo data");
    Attributes.setString(e, "bar", "bar data");
    Attributes.setString(e, "baz", "baz data");
    snk.append(e);
    snk.close();
  }

}
