/*
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
package org.apache.flume.source.http;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServletRequest;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TestBLOBHandler {

  HTTPSourceHandler handler;

  @Before
  public void setUp() {
    handler = new BLOBHandler();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testCSVData() throws Exception {
    Map requestParameterMap = new HashMap();
    requestParameterMap.put("param1", new String[] { "value1" });
    requestParameterMap.put("param2", new String[] { "value2" });

    HttpServletRequest req = mock(HttpServletRequest.class);
    final String csvData = "a,b,c";

    ServletInputStream servletInputStream = new DelegatingServletInputStream(
        new ByteArrayInputStream(csvData.getBytes()));

    when(req.getInputStream()).thenReturn(servletInputStream);
    when(req.getParameterMap()).thenReturn(requestParameterMap);

    Context context = mock(Context.class);
    when(
        context.getString(BLOBHandler.MANDATORY_PARAMETERS,
            BLOBHandler.DEFAULT_MANDATORY_PARAMETERS)).thenReturn(
        "param1,param2");

    handler.configure(context);
    List<Event> deserialized = handler.getEvents(req);
    assertEquals(1, deserialized.size());
    Event e = deserialized.get(0);

    assertEquals(new String(e.getBody()), csvData);
    assertEquals(e.getHeaders().get("param1"), "value1");
    assertEquals(e.getHeaders().get("param2"), "value2");
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testTabData() throws Exception {
    Map requestParameterMap = new HashMap();
    requestParameterMap.put("param1", new String[] { "value1" });

    HttpServletRequest req = mock(HttpServletRequest.class);
    final String tabData = "a\tb\tc";

    ServletInputStream servletInputStream = new DelegatingServletInputStream(
        new ByteArrayInputStream(tabData.getBytes()));

    when(req.getInputStream()).thenReturn(servletInputStream);
    when(req.getParameterMap()).thenReturn(requestParameterMap);

    Context context = mock(Context.class);
    when(
        context.getString(BLOBHandler.MANDATORY_PARAMETERS,
            BLOBHandler.DEFAULT_MANDATORY_PARAMETERS)).thenReturn("param1");

    handler.configure(context);

    List<Event> deserialized = handler.getEvents(req);
    assertEquals(1, deserialized.size());
    Event e = deserialized.get(0);

    assertEquals(new String(e.getBody()), tabData);
    assertEquals(e.getHeaders().get("param1"), "value1");
  }

  @SuppressWarnings({ "rawtypes" })
  @Test(expected = IllegalArgumentException.class)
  public void testMissingParameters() throws Exception {
    Map requestParameterMap = new HashMap();

    HttpServletRequest req = mock(HttpServletRequest.class);
    final String tabData = "a\tb\tc";

    ServletInputStream servletInputStream = new DelegatingServletInputStream(
        new ByteArrayInputStream(tabData.getBytes()));

    when(req.getInputStream()).thenReturn(servletInputStream);
    when(req.getParameterMap()).thenReturn(requestParameterMap);

    Context context = mock(Context.class);
    when(
        context.getString(BLOBHandler.MANDATORY_PARAMETERS,
            BLOBHandler.DEFAULT_MANDATORY_PARAMETERS)).thenReturn("param1");

    handler.configure(context);

    handler.getEvents(req);

  }

  class DelegatingServletInputStream extends ServletInputStream {

    private final InputStream sourceStream;

    /**
     * Create a DelegatingServletInputStream for the given source stream.
     *
     * @param sourceStream
     *          the source stream (never <code>null</code>)
     */
    public DelegatingServletInputStream(InputStream sourceStream) {
      this.sourceStream = sourceStream;
    }

    /**
     * Return the underlying source stream (never <code>null</code>).
     */
    public final InputStream getSourceStream() {
      return this.sourceStream;
    }

    public int read() throws IOException {
      return this.sourceStream.read();
    }

    public void close() throws IOException {
      super.close();
      this.sourceStream.close();
    }

  }

}
