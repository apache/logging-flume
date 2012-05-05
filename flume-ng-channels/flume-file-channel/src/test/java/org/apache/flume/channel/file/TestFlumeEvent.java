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
package org.apache.flume.channel.file;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;

public class TestFlumeEvent {

  @Test
  public void testBasics() {
    Map<String, String> headers = Maps.newHashMap();
    headers.put("key", "value");
    byte[] body = "flume".getBytes(Charsets.UTF_8);
    FlumeEvent event = new FlumeEvent(headers, body);
    Assert.assertEquals(headers, event.getHeaders());
    Assert.assertTrue(Arrays.equals(body, event.getBody()));
  }

  @Test
  public void testSerialization() throws IOException {
    Map<String, String> headers = Maps.newHashMap();
    headers.put("key", "value");
    byte[] body = "flume".getBytes(Charsets.UTF_8);
    FlumeEvent in = new FlumeEvent(headers, body);
    FlumeEvent out = FlumeEvent.from(TestUtils.toDataInput(in));
    Assert.assertEquals(headers, out.getHeaders());
    Assert.assertTrue(Arrays.equals(body, out.getBody()));
    in.setHeaders(null);
    in.setBody(null);
    out = FlumeEvent.from(TestUtils.toDataInput(in));
    Assert.assertEquals(Maps.newHashMap(), out.getHeaders());
    Assert.assertNull(out.getBody());
  }
}
