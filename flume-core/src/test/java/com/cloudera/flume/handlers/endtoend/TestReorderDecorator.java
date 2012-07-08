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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.debug.ConsoleEventSink;
import com.cloudera.flume.handlers.debug.MemorySinkSource;

/**
 * This just verifies that events have been reordered
 */
public class TestReorderDecorator {
  // first version for me to see the order
  @Test
  public void testReorderDecorator() throws IOException, InterruptedException {
    ReorderDecorator<EventSink> reorder =
        new ReorderDecorator<EventSink>(new ConsoleEventSink(), .5, .5, 0);
    reorder.open();
    for (int i = 0; i < 10; i++) {
      Event e = new EventImpl(new byte[0]);
      e.set("order", ByteBuffer.allocate(4).putInt(i).array());
      reorder.append(e);
    }
    System.out.println("closing");
    reorder.close();
  }

  // this version checks to make sure it remains the same.
  @Test
  public void testReorderDecoratorCheck() throws IOException,
      InterruptedException {
    MemorySinkSource mss = new MemorySinkSource();
    ReorderDecorator<EventSink> reorder =
        new ReorderDecorator<EventSink>(mss, .5, .5, 0);
    reorder.open();
    for (int i = 0; i < 10; i++) {
      Event e = new EventImpl(new byte[0]);
      e.set("order", ByteBuffer.allocate(4).putInt(i).array());
      reorder.append(e);
    }
    System.out.println("closing");
    reorder.close();
    int[] order = { 0, 2, 1, 4, 3, 5, 7, 6, 8, 9 };
    for (int i = 0; i < order.length; i++) {
      Event e = mss.next();
      int j = ByteBuffer.wrap(e.get("order")).asIntBuffer().get();
      assertEquals(j, order[i]);
    }

  }

  @Test
  public void testBuilder() {
    ReorderDecorator.builder().build(new Context(), "0.0", "0.0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadBuilder1() {
    ReorderDecorator.builder().build(new Context(), "-1.0", "0.0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadBuilder2() {
    ReorderDecorator.builder().build(new Context(), "2.0", "0.0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadBuilder3() {
    ReorderDecorator.builder().build(new Context(), "0.0", "-1.0");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadBuilder4() {
    ReorderDecorator.builder().build(new Context(), "0.0", "2.0");
  }

}
