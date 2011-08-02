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

import java.util.Arrays;
import java.util.Map.Entry;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;

public class TestThriftEventAdaptor {

  private Event testEvent;

  @Before
  public void setUp() {
    testEvent = new EventImpl();

    testEvent.set("one", "one".getBytes());
    testEvent.set("two", "two".getBytes());
  }

  @Test
  public void testConvert() {
    ThriftFlumeEvent thriftEvent = ThriftEventAdaptor.convert(testEvent);

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
    ThriftFlumeEvent thriftEvent = ThriftEventAdaptor.convert(testEvent);

    Assert.assertNotNull(thriftEvent);
    Assert
        .assertNull(new ThriftEventAdaptor(thriftEvent).get("i do not exist"));
  }

  @Ignore
  @Test
  public void testNullBody() {
    ThriftFlumeEvent tEvt = new ThriftFlumeEvent(); // null body
    Assert.assertEquals(null, tEvt.body);
    Assert.assertEquals(0, new ThriftEventAdaptor(tEvt).getBody().length);
  }

}
