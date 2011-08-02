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

import com.cloudera.flume.handlers.debug.MemorySinkSource;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestFormatterDecorator {

  /**
   * Test that an event's body is correctly formatted, and that attributes
   * are correctly passed through.   
   */
  @Test
  public void testFormat() throws IOException{
    MemorySinkSource mem = new MemorySinkSource();
    // %b will be replaced with event body
    EventSink format = new FormatterDecorator<EventSink>(mem, "test %{body}");

    Event e = new EventImpl("content".getBytes());
    e.set("attr1", "value".getBytes());
    format.open();
    format.append(e);
    format.close();

    mem.open();
    Event e2 = mem.next();
    assertEquals("test content", new String(e2.getBody()));
    assertEquals("value", Attributes.readString(e2, "attr1"));
  }
}
