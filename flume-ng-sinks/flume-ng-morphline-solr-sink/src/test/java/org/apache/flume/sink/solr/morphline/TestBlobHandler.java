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

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.flume.Event;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestBlobHandler extends Assert {

  private HTTPSourceHandler handler;

  @Before
  public void setUp() {
    handler = new BlobHandler();
  }

  @Test
  public void testSingleEvent() throws Exception {
    byte[] json = "foo".getBytes("UTF-8");
    HttpServletRequest req = new FlumeHttpServletRequestWrapper(json);
    List<Event> deserialized = handler.getEvents(req);
    assertEquals(1,  deserialized.size());
    Event e = deserialized.get(0);
    assertEquals(0, e.getHeaders().size());
    assertEquals("foo", new String(e.getBody(),"UTF-8"));
  }

  @Test
  public void testEmptyEvent() throws Exception {
    byte[] json = "".getBytes("UTF-8");
    HttpServletRequest req = new FlumeHttpServletRequestWrapper(json);
    List<Event> deserialized = handler.getEvents(req);
    assertEquals(1,  deserialized.size());
    Event e = deserialized.get(0);
    assertEquals(0, e.getHeaders().size());
    assertEquals("", new String(e.getBody(),"UTF-8"));
  }

}
