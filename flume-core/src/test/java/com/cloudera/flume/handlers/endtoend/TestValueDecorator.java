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
package com.cloudera.flume.handlers.endtoend;

import java.io.IOException;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.handlers.debug.ConsoleEventSink;
import org.junit.Assert;
import org.junit.Test;

/**
 * Some tests to verify value decorator behaviour
 */
public class TestValueDecorator {
  @Test
  public void testValueDecorator() throws IOException, InterruptedException {
    String value = "value%{nanos}";
    ValueDecorator dec = new ValueDecorator(new ConsoleEventSink(), "attr", value, false);
    dec.open();
    Event e = new EventImpl("body".getBytes(), 1234567L, Event.Priority.INFO, 87654321L, "host");

    dec.append(e);

    Assert.assertNotNull("Attribute wasn't added", e.get("attr"));
    // Attribute value should NOT be escaped
    Assert.assertArrayEquals("Attribute value is incorrect", value.getBytes(), e.get("attr"));

    dec.close();
  }

  @Test
  public void testValueDecoratorWithEscape() throws IOException, InterruptedException {
    String value = "value-%{nanos}-%{body}";
    ValueDecorator dec = new ValueDecorator(new ConsoleEventSink(), "attr", value, true);
    dec.open();
    Event e = new EventImpl("bodyString".getBytes(), 1234567L, Event.Priority.INFO, 87654321L, "host");

    dec.append(e);

    Assert.assertNotNull("Attribute wasn't added", e.get("attr"));
    Assert.assertEquals("Attribute value is incorrect", "value-87654321-bodyString", new String(e.get("attr")));

    dec.close();
  }
}

