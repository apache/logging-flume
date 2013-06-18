/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.solr.morphline;

import java.io.IOException;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventDeserializer;
import org.apache.flume.serialization.EventDeserializerFactory;
import org.apache.flume.serialization.ResettableInputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TestBlobDeserializer extends Assert {

  private String mini;

  @Before
  public void setup() {
    StringBuilder sb = new StringBuilder();
    sb.append("line 1\n");
    sb.append("line 2\n");
    mini = sb.toString();
  }

  @Test
  public void testSimple() throws IOException {
    ResettableInputStream in = new ResettableTestStringInputStream(mini);
    EventDeserializer des = new BlobDeserializer(new Context(), in);
    validateMiniParse(des);
  }

  @Test
  public void testSimpleViaBuilder() throws IOException {
    ResettableInputStream in = new ResettableTestStringInputStream(mini);
    EventDeserializer.Builder builder = new BlobDeserializer.Builder();
    EventDeserializer des = builder.build(new Context(), in);
    validateMiniParse(des);
  }

  @Test
  public void testSimpleViaFactory() throws IOException {
    ResettableInputStream in = new ResettableTestStringInputStream(mini);
    EventDeserializer des;
    des = EventDeserializerFactory.getInstance(BlobDeserializer.Builder.class.getName(), new Context(), in);
    validateMiniParse(des);
  }

  @Test
  public void testBatch() throws IOException {
    ResettableInputStream in = new ResettableTestStringInputStream(mini);
    EventDeserializer des = new BlobDeserializer(new Context(), in);
    List<Event> events;

    events = des.readEvents(10); // try to read more than we should have
    assertEquals(1, events.size());
    assertEventBodyEquals(mini, events.get(0));

    des.mark();
    des.close();
  }

  // truncation occurs at maxLineLength boundaries
  @Test
  public void testMaxLineLength() throws IOException {
    String longLine = "abcdefghijklmnopqrstuvwxyz\n";
    Context ctx = new Context();
    ctx.put(BlobDeserializer.MAX_BLOB_LENGTH_KEY, "10");

    ResettableInputStream in = new ResettableTestStringInputStream(longLine);
    EventDeserializer des = new BlobDeserializer(ctx, in);

    assertEventBodyEquals("abcdefghij", des.readEvent());
    assertEventBodyEquals("klmnopqrst", des.readEvent());
    assertEventBodyEquals("uvwxyz\n", des.readEvent());
    assertNull(des.readEvent());
  }

  private void assertEventBodyEquals(String expected, Event event) {
    String bodyStr = new String(event.getBody(), Charsets.UTF_8);
    assertEquals(expected, bodyStr);
  }

  private void validateMiniParse(EventDeserializer des) throws IOException {
    Event evt;

    des.mark();
    evt = des.readEvent();
    assertEquals(new String(evt.getBody()), mini);
    des.reset(); // reset!

    evt = des.readEvent();
    assertEquals("data should be repeated, " +
        "because we reset() the stream", new String(evt.getBody()), mini);

    evt = des.readEvent();
    assertNull("Event should be null because there are no lines " +
        "left to read", evt);

    des.mark();
    des.close();
  }
}
