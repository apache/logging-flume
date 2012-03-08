/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.flume.channel.jdbc;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.channel.jdbc.impl.PersistableEvent;
import org.junit.Assert;
import org.junit.Test;

public class TestPersistentEvent {

  @Test
  public void testMarshalling() {

    int nameLimit = ConfigurationConstants.HEADER_NAME_LENGTH_THRESHOLD;
    int valLimit = ConfigurationConstants.HEADER_VALUE_LENGTH_THRESHOLD;

    byte[] s1 = MockEventUtils.generatePayload(1);
    runTest(s1, null);

    byte[] s2 = MockEventUtils.generatePayload(2);
    runTest(s2, new HashMap<String, String>());

    int th = ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD;

    byte[] s3 = MockEventUtils.generatePayload(th - 2);
    Map<String, String> m3 = new HashMap<String, String>();
    m3.put(MockEventUtils.generateHeaderString(1),
        MockEventUtils.generateHeaderString(1));
    runTest(s3, m3);

    byte[] s4 = MockEventUtils.generatePayload(th - 1);
    Map<String, String> m4 = new HashMap<String, String>();
    m4.put(MockEventUtils.generateHeaderString(nameLimit - 21), "w");
    m4.put(MockEventUtils.generateHeaderString(nameLimit - 2), "x");
    m4.put(MockEventUtils.generateHeaderString(nameLimit - 1), "y");
    m4.put(MockEventUtils.generateHeaderString(nameLimit), "z");
    m4.put(MockEventUtils.generateHeaderString(nameLimit + 1), "a");
    m4.put(MockEventUtils.generateHeaderString(nameLimit + 2), "b");
    m4.put(MockEventUtils.generateHeaderString(nameLimit + 21), "c");
    runTest(s4, m4);

    byte[] s5 = MockEventUtils.generatePayload(th);
    Map<String, String> m5 = new HashMap<String, String>();
    m5.put("w", MockEventUtils.generateHeaderString(valLimit - 21));
    m5.put("x", MockEventUtils.generateHeaderString(valLimit - 2));
    m5.put("y", MockEventUtils.generateHeaderString(valLimit - 1));
    m5.put("z", MockEventUtils.generateHeaderString(valLimit));
    m5.put("a", MockEventUtils.generateHeaderString(valLimit + 1));
    m5.put("b", MockEventUtils.generateHeaderString(valLimit + 2));
    m5.put("c", MockEventUtils.generateHeaderString(valLimit + 21));
    runTest(s5, m5);

    byte[] s6 = MockEventUtils.generatePayload(th + 1);
    Map<String, String> m6 = new HashMap<String, String>();
    m6.put(MockEventUtils.generateHeaderString(nameLimit - 21),
           MockEventUtils.generateHeaderString(valLimit - 21));
    m6.put(MockEventUtils.generateHeaderString(nameLimit - 2),
           MockEventUtils.generateHeaderString(valLimit - 2));
    m6.put(MockEventUtils.generateHeaderString(nameLimit - 1),
           MockEventUtils.generateHeaderString(valLimit - 1));
    m6.put(MockEventUtils.generateHeaderString(nameLimit),
           MockEventUtils.generateHeaderString(valLimit));
    m6.put(MockEventUtils.generateHeaderString(nameLimit + 1),
           MockEventUtils.generateHeaderString(valLimit + 1));
    m6.put(MockEventUtils.generateHeaderString(nameLimit + 2),
           MockEventUtils.generateHeaderString(valLimit + 2));
    m6.put(MockEventUtils.generateHeaderString(nameLimit + 21),
           MockEventUtils.generateHeaderString(valLimit + 21));

    runTest(s6, m6);

    byte[] s7 = MockEventUtils.generatePayload(th + 2);
    runTest(s7, null);

    byte[] s8 = MockEventUtils.generatePayload(th + 27);
    runTest(s8, null);
  }


  private void runTest(byte[] payload, Map<String, String> headers) {
    PersistableEvent pe = new PersistableEvent("test",
        new MockEvent(payload, headers, null));
    Assert.assertArrayEquals(payload, pe.getBody());
    Map<String, String> h = pe.getHeaders();
    if (h == null) {
      Assert.assertTrue(headers == null || headers.size() == 0);
    } else {
      Assert.assertTrue(headers.size() == h.size());
      for (String key : h.keySet()) {
        Assert.assertTrue(headers.containsKey(key));
        String value = h.get(key);
        String expectedValue = headers.remove(key);
        Assert.assertEquals(expectedValue, value);
      }
      Assert.assertTrue(headers.size() == 0);
    }
  }
}
