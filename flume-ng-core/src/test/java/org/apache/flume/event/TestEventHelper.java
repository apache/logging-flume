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

import org.junit.Assert;
import org.junit.Test;

public class TestEventHelper {

  @Test
  public void testPrintable() {
    SimpleEvent event = new SimpleEvent();
    event.setBody("Some text".getBytes());
    String eventDump = EventHelper.dumpEvent(event);
    System.out.println(eventDump);
    Assert.assertTrue(eventDump, eventDump.contains("Some text"));
  }

  @Test
  public void testNonPrintable() {
    SimpleEvent event = new SimpleEvent();
    byte[] body = new byte[5];
    event.setBody(body);
    String eventDump = EventHelper.dumpEvent(event);
    Assert.assertTrue(eventDump, eventDump.contains("....."));
  }
}
