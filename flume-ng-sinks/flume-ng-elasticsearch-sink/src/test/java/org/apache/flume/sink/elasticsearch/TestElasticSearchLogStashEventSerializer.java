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
package org.apache.flume.sink.elasticsearch;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Test;

import java.util.Date;
import java.util.Map;

import com.google.gson.JsonParser;
import com.google.gson.JsonElement;

import static org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer.charset;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;

public class TestElasticSearchLogStashEventSerializer {

  @Test
  public void testRoundTrip() throws Exception {
    ElasticSearchLogStashEventSerializer fixture = new ElasticSearchLogStashEventSerializer();
    Context context = new Context();
    fixture.configure(context);

    String message = "test body";
    Map<String, String> headers = Maps.newHashMap();
    long timestamp = System.currentTimeMillis();
    headers.put("timestamp", String.valueOf(timestamp));
    headers.put("source", "flume_tail_src");
    headers.put("host", "test@localhost");
    headers.put("src_path", "/tmp/test");
    headers.put("headerNameOne", "headerValueOne");
    headers.put("headerNameTwo", "headerValueTwo");
    headers.put("type", "sometype");
    Event event = EventBuilder.withBody(message.getBytes(charset));
    event.setHeaders(headers);

    XContentBuilder expected = jsonBuilder()
        .startObject();
            expected.field("@message", new String(message.getBytes(), charset));
            expected.field("@timestamp", new Date(timestamp));
            expected.field("@source", "flume_tail_src");
            expected.field("@type", "sometype");
            expected.field("@source_host", "test@localhost");
            expected.field("@source_path", "/tmp/test");

            expected.startObject("@fields");
                expected.field("timestamp", String.valueOf(timestamp));
                expected.field("src_path", "/tmp/test");
                expected.field("host", "test@localhost");
                expected.field("headerNameTwo", "headerValueTwo");
                expected.field("source", "flume_tail_src");
                expected.field("headerNameOne", "headerValueOne");
                expected.field("type", "sometype");
            expected.endObject();

        expected.endObject();

    XContentBuilder actual = fixture.getContentBuilder(event);
    
    JsonParser parser = new JsonParser();
    assertEquals(parser.parse(expected.string()),parser.parse(actual.string()));
  }

  @Test
  public void shouldHandleInvalidJSONDuringComplexParsing() throws Exception {
    ElasticSearchLogStashEventSerializer fixture = new ElasticSearchLogStashEventSerializer();
    Context context = new Context();
    fixture.configure(context);

    String message = "{flume: somethingnotvalid}";
    Map<String, String> headers = Maps.newHashMap();
    long timestamp = System.currentTimeMillis();
    headers.put("timestamp", String.valueOf(timestamp));
    headers.put("source", "flume_tail_src");
    headers.put("host", "test@localhost");
    headers.put("src_path", "/tmp/test");
    headers.put("headerNameOne", "headerValueOne");
    headers.put("headerNameTwo", "headerValueTwo");
    headers.put("type", "sometype");
    Event event = EventBuilder.withBody(message.getBytes(charset));
    event.setHeaders(headers);

    XContentBuilder expected = jsonBuilder().
        startObject();
            expected.field("@message", new String(message.getBytes(), charset));
            expected.field("@timestamp", new Date(timestamp));
            expected.field("@source", "flume_tail_src");
            expected.field("@type", "sometype");
            expected.field("@source_host", "test@localhost");
            expected.field("@source_path", "/tmp/test");

            expected.startObject("@fields");
                expected.field("timestamp", String.valueOf(timestamp));
                expected.field("src_path", "/tmp/test");
                expected.field("host", "test@localhost");
                expected.field("headerNameTwo", "headerValueTwo");
                expected.field("source", "flume_tail_src");
                expected.field("headerNameOne", "headerValueOne");
                expected.field("type", "sometype");
            expected.endObject();

        expected.endObject();

    XContentBuilder actual = fixture.getContentBuilder(event);

    JsonParser parser = new JsonParser();
    assertEquals(parser.parse(expected.string()),parser.parse(actual.string()));
  }
}
