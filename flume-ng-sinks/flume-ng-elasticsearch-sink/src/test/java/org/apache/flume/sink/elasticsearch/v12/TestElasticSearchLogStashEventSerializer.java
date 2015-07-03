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
package org.apache.flume.sink.elasticsearch.v12;

import com.google.gson.JsonParser;
import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.joda.time.DateTimeUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;
import static org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer.charset;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.*;

public class TestElasticSearchLogStashEventSerializer {

    @Test
    public void testSimpleMessageTimeStampHeader() throws IOException {
        ElasticSearchLogStashEventSerializer sut = new ElasticSearchLogStashEventSerializer();
        Context context = new Context();
        sut.configure(context);
        //create the Event
        String message = "event message";
        Map<String, String> headers = Maps.newHashMap();
        long timestamp = System.currentTimeMillis();
        headers.put("timestamp", String.valueOf(timestamp));
        Event event = EventBuilder.withBody(message.getBytes(charset));
        event.setHeaders(headers);
        //create the expected value
        XContentBuilder expected = jsonBuilder().startObject();
        expected.field("message", new String(message.getBytes(), charset));
        expected.field("@timestamp", new Date(timestamp));
        expected.endObject();

        //compare
        XContentBuilder actual = sut.getContentBuilder(event);

        JsonParser parser = new JsonParser();
        assertEquals(parser.parse(expected.string()), parser.parse(actual.string()));
    }

    @Test
    public void testSimpleMessageTimeStampAnnotationHeader() throws IOException {
        ElasticSearchLogStashEventSerializer sut = new ElasticSearchLogStashEventSerializer();
        Context context = new Context();
        sut.configure(context);
        //create the Event
        String message = "event message";
        Map<String, String> headers = Maps.newHashMap();
        long timestamp = System.currentTimeMillis();
        headers.put("@timestamp", String.valueOf(timestamp));
        Event event = EventBuilder.withBody(message.getBytes(charset));
        event.setHeaders(headers);
        //create the expected value
        XContentBuilder expected = jsonBuilder().startObject();
        expected.field("message", new String(message.getBytes(), charset));
        expected.field("@timestamp", new Date(timestamp));
        expected.endObject();

        //compare
        XContentBuilder actual = sut.getContentBuilder(event);

        JsonParser parser = new JsonParser();
        assertEquals(parser.parse(expected.string()), parser.parse(actual.string()));
    }

    @Test
    public void testSimpleMessageWithoutTimestampHeader() throws IOException {
        ElasticSearchLogStashEventSerializer sut = new ElasticSearchLogStashEventSerializer();
        Context context = new Context();
        sut.configure(context);
        //create the Event
        String message = "event message";
        Map<String, String> headers = Maps.newHashMap();
        long timestamp = System.currentTimeMillis();
        headers.put("anotherHeader", String.valueOf(timestamp));
        Event event = EventBuilder.withBody(message.getBytes(charset));
        event.setHeaders(headers);
        //create the expected value
        XContentBuilder expected = jsonBuilder().startObject();
        expected.field("message", new String(message.getBytes(), charset));
        expected.field("anotherHeader", timestamp);
        expected.endObject();

        //compare
        XContentBuilder actual = sut.getContentBuilder(event);

        JsonParser parser = new JsonParser();
        assertEquals(parser.parse(expected.string()), parser.parse(actual.string()));
    }


    @Test
    public void testMessageWithHeaders() throws IOException {
        ElasticSearchLogStashEventSerializer sut = new ElasticSearchLogStashEventSerializer();
        Context context = new Context();
        sut.configure(context);
        //create the Event
        String message = "event message";
        Map<String, String> headers = Maps.newHashMap();
        long timestamp = System.currentTimeMillis();
        headers.put("@timestamp", String.valueOf(timestamp));
        headers.put("strHeader", "strHeaderValue");
        headers.put("longHeader", "12");
        headers.put("decimalHeader", "15345.123455");
        Event event = EventBuilder.withBody(message.getBytes(charset));
        event.setHeaders(headers);
        //create the expected value
        XContentBuilder expected = jsonBuilder().startObject();
        expected.field("message", new String(message.getBytes(), charset));
        expected.field("@timestamp", new Date(timestamp));
        expected.field("strHeader", "strHeaderValue");
        expected.field("decimalHeader", 15345.123455);
        expected.field("longHeader", 12L);
        expected.endObject();

        //compare
        XContentBuilder actual = sut.getContentBuilder(event);

        JsonParser parser = new JsonParser();
        assertEquals(parser.parse(expected.string()), parser.parse(actual.string()));
    }
}