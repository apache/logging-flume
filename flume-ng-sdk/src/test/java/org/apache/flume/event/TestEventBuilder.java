/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.event;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;
import org.junit.Assert;
import org.junit.Test;

public class TestEventBuilder {

  @Test
  public void testBody() {
    Event e1 = EventBuilder.withBody("e1".getBytes());
    Assert.assertNotNull(e1);
    Assert.assertArrayEquals("body is correct", "e1".getBytes(), e1.getBody());

    Event e2 = EventBuilder.withBody(Long.valueOf(2).toString().getBytes());
    Assert.assertNotNull(e2);
    Assert.assertArrayEquals("body is correct", Long.valueOf(2L).toString()
        .getBytes(), e2.getBody());
  }

  @Test
  public void testHeaders() {
    Map<String, String> headers = new HashMap<String, String>();

    headers.put("one", "1");
    headers.put("two", "2");

    Event e1 = EventBuilder.withBody("e1".getBytes(), headers);

    Assert.assertNotNull(e1);
    Assert.assertArrayEquals("e1 has the proper body", "e1".getBytes(),
        e1.getBody());
    Assert.assertEquals("e1 has the proper headers", 2, e1.getHeaders().size());
    Assert.assertEquals("e1 has a one key", "1", e1.getHeaders().get("one"));
  }

}
