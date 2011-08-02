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
package com.cloudera.flume.handlers.thrift;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;

public class TestThriftEventConvertUtil {

  private Event testEvent;

  @Before
  public void setUp() {
    testEvent = new EventImpl();

    testEvent.set("one", "one".getBytes());
    testEvent.set("two", "two".getBytes());
  }

  @Test
  public void testConvert() {
    ThriftFlumeEvent thriftEvent = ThriftEventConvertUtil.toThriftEvent(testEvent);

    Assert.assertNotNull(thriftEvent);
    Assert.assertNotNull(thriftEvent.host);
    Assert.assertNotNull(thriftEvent.timestamp);
    Assert.assertNotNull(thriftEvent.fields);
    Assert.assertNotNull(thriftEvent.priority);

    for (Entry<String, byte[]> entry : testEvent.getAttrs().entrySet()) {
      Assert.assertTrue(thriftEvent.fields.containsKey(entry.getKey()));
      Assert.assertTrue(Arrays.equals(thriftEvent.fields.get(entry.getKey())
          .array(), entry.getValue()));
    }
  }

  @Test
  public void testInvalidAttribute() {
    ThriftFlumeEvent thriftEvent = ThriftEventConvertUtil.toThriftEvent(testEvent);

    Assert.assertNotNull(thriftEvent);
    Assert.assertNull(ThriftEventConvertUtil.toFlumeEvent(thriftEvent).get(
        "i do not exist"));
  }

  @Test
  public void testNullBody() {
    ThriftFlumeEvent tEvt = new ThriftFlumeEvent(); // null body
    Assert.assertEquals(null, tEvt.body);
    Assert.assertEquals(0,
        ThriftEventConvertUtil.toFlumeEvent(tEvt).getBody().length);
  }

  /**
   * This event is evil because it surpasses the max event size.
   */
  @Test(expected = RuntimeException.class)
  public void testEvilEvent() {
    long maxSize = FlumeConfiguration.get().getEventMaxSizeBytes();
    ByteBuffer toobig = ByteBuffer.allocate((int) (maxSize * 2));
    ThriftFlumeEvent tevt = new ThriftFlumeEvent(0L, Priority.INFO, toobig, 0L,
        "localhost", new HashMap<String, ByteBuffer>());
    Event e = ThriftEventConvertUtil.toFlumeEvent(tevt);
    EventImpl.select(e);
  }

  /**
   * This event is evil because it surpasses the max event size.
   */
  @Test
  public void testTruncEvilEvent() {
    long maxSize = FlumeConfiguration.get().getEventMaxSizeBytes();
    ByteBuffer toobig = ByteBuffer.allocate((int) (maxSize * 2));
    ThriftFlumeEvent tevt = new ThriftFlumeEvent(0L, Priority.INFO, toobig, 0L,
        "localhost", new HashMap<String, ByteBuffer>());
    Event e = ThriftEventConvertUtil.toFlumeEvent(tevt, true);
    EventImpl.select(e);
  }

}
