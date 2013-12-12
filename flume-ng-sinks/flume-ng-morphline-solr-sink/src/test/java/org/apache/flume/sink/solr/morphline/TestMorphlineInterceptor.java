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

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Test;

import org.kitesdk.morphline.base.Fields;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

public class TestMorphlineInterceptor extends Assert {

  private static final String RESOURCES_DIR = "target/test-classes";

  @Test
  public void testNoOperation() throws Exception {
    Context context = new Context();
    context.put(MorphlineHandlerImpl.MORPHLINE_FILE_PARAM, RESOURCES_DIR + "/test-morphlines/noOperation.conf");
    Event input = EventBuilder.withBody("foo", Charsets.UTF_8);
    input.getHeaders().put("name", "nadja");
    MorphlineInterceptor interceptor = build(context);
    Event actual = interceptor.intercept(input);
    interceptor.close();
    Event expected = EventBuilder.withBody("foo".getBytes("UTF-8"), ImmutableMap.of("name", "nadja"));
    assertEqualsEvent(expected, actual);
    
    List<Event> actualList = build(context).intercept(Collections.singletonList(input));
    List<Event> expectedList = Collections.singletonList(expected);
    assertEqualsEventList(expectedList, actualList);
  }

  @Test
  public void testReadClob() throws Exception {
    Context context = new Context();
    context.put(MorphlineHandlerImpl.MORPHLINE_FILE_PARAM, RESOURCES_DIR + "/test-morphlines/readClob.conf");
    Event input = EventBuilder.withBody("foo", Charsets.UTF_8);
    input.getHeaders().put("name", "nadja");
    Event actual = build(context).intercept(input);
    Event expected = EventBuilder.withBody(null, ImmutableMap.of("name", "nadja", Fields.MESSAGE, "foo"));
    assertEqualsEvent(expected, actual);

    List<Event> actualList = build(context).intercept(Collections.singletonList(input));
    List<Event> expectedList = Collections.singletonList(expected);
    assertEqualsEventList(expectedList, actualList);
  }

  @Test
  public void testGrokIfNotMatchDropEventRetain() throws Exception {
    Context context = new Context();
    context.put(MorphlineHandlerImpl.MORPHLINE_FILE_PARAM, RESOURCES_DIR + "/test-morphlines/grokIfNotMatchDropRecord.conf");

    String msg = "<164>Feb  4 10:46:14 syslog sshd[607]: Server listening on 0.0.0.0 port 22.";
    Event input = EventBuilder.withBody(null, ImmutableMap.of(Fields.MESSAGE, msg));
    Event actual = build(context).intercept(input);

    Map<String, String> expected = new HashMap();
    expected.put(Fields.MESSAGE, msg);
    expected.put("syslog_pri", "164");
    expected.put("syslog_timestamp", "Feb  4 10:46:14");
    expected.put("syslog_hostname", "syslog");
    expected.put("syslog_program", "sshd");
    expected.put("syslog_pid", "607");
    expected.put("syslog_message", "Server listening on 0.0.0.0 port 22.");
    Event expectedEvent = EventBuilder.withBody(null, expected);
    assertEqualsEvent(expectedEvent, actual);
  }

  @Test
  /* leading XXXXX does not match regex, thus we expect the event to be dropped */
  public void testGrokIfNotMatchDropEventDrop() throws Exception {
    Context context = new Context();
    context.put(MorphlineHandlerImpl.MORPHLINE_FILE_PARAM, RESOURCES_DIR + "/test-morphlines/grokIfNotMatchDropRecord.conf");
    String msg = "<XXXXXXXXXXXXX164>Feb  4 10:46:14 syslog sshd[607]: Server listening on 0.0.0.0 port 22.";
    Event input = EventBuilder.withBody(null, ImmutableMap.of(Fields.MESSAGE, msg));
    Event actual = build(context).intercept(input);
    assertNull(actual);
  }

  @Test
  /** morphline says route to southpole if it's an avro file, otherwise route to northpole */
  public void testIfDetectMimeTypeRouteToSouthPole() throws Exception {
    Context context = new Context();
    context.put(MorphlineHandlerImpl.MORPHLINE_FILE_PARAM, RESOURCES_DIR + "/test-morphlines/ifDetectMimeType.conf");
    context.put(MorphlineHandlerImpl.MORPHLINE_VARIABLE_PARAM + ".MY.MIME_TYPE", "avro/binary");

    Event input = EventBuilder.withBody(Files.toByteArray(new File(RESOURCES_DIR + "/test-documents/sample-statuses-20120906-141433.avro")));
    Event actual = build(context).intercept(input);

    Map<String, String> expected = new HashMap();
    expected.put(Fields.ATTACHMENT_MIME_TYPE, "avro/binary");
    expected.put("flume.selector.header", "goToSouthPole");
    Event expectedEvent = EventBuilder.withBody(input.getBody(), expected);
    assertEqualsEvent(expectedEvent, actual);
  }

  @Test
  /** morphline says route to southpole if it's an avro file, otherwise route to northpole */
  public void testIfDetectMimeTypeRouteToNorthPole() throws Exception {
    Context context = new Context();
    context.put(MorphlineHandlerImpl.MORPHLINE_FILE_PARAM, RESOURCES_DIR + "/test-morphlines/ifDetectMimeType.conf");
    context.put(MorphlineHandlerImpl.MORPHLINE_VARIABLE_PARAM + ".MY.MIME_TYPE", "avro/binary");

    Event input = EventBuilder.withBody(Files.toByteArray(new File(RESOURCES_DIR + "/test-documents/testPDF.pdf")));
    Event actual = build(context).intercept(input);

    Map<String, String> expected = new HashMap();
    expected.put(Fields.ATTACHMENT_MIME_TYPE, "application/pdf");
    expected.put("flume.selector.header", "goToNorthPole");
    Event expectedEvent = EventBuilder.withBody(input.getBody(), expected);
    assertEqualsEvent(expectedEvent, actual);
  }

  private MorphlineInterceptor build(Context context) {
    MorphlineInterceptor.Builder builder = new MorphlineInterceptor.Builder();
    builder.configure(context);
    return builder.build();
  }
  
  private void assertEqualsEvent(Event x, Event y) { // b/c SimpleEvent doesn't implement equals() method :-(
    assertEquals(x.getHeaders(), y.getHeaders());
    assertArrayEquals(x.getBody(), y.getBody());
  }

  private void assertEqualsEventList(List<Event> x, List<Event> y) {
    assertEquals(x.size(), y.size());
    for (int i = 0; i < x.size(); i++) {
      assertEqualsEvent(x.get(i), y.get(i));      
    }
  }

}
