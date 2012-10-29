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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import java.io.BufferedReader;
import java.lang.reflect.Type;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.JSONEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * JSONHandler for HTTPSource that accepts an array of events.
 *
 * This handler throws exception if the deserialization fails because of bad
 * format or any other reason.
 *
 *
 * Each event must be encoded as a map with two key-value pairs. <p> 1. headers
 * - the key for this key-value pair is "headers". The value for this key is
 * another map, which represent the event headers. These headers are inserted
 * into the Flume event as is. <p> 2. body - The body is a string which
 * represents the body of the event. The key for this key-value pair is "body".
 * All key-value pairs are considered to be headers. An example: <p> [{"headers"
 * : {"a":"b", "c":"d"},"body": "random_body"}, {"headers" : {"e": "f"},"body":
 * "random_body2"}] <p> would be interpreted as the following two flume events:
 * <p> * Event with body: "random_body" (in UTF-8/UTF-16/UTF-32 encoded bytes)
 * and headers : (a:b, c:d) <p> *
 * Event with body: "random_body2" (in UTF-8/UTF-16/UTF-32 encoded bytes) and
 * headers : (e:f) <p>
 *
 * The charset of the body is read from the request and used. If no charset is
 * set in the request, then the charset is assumed to be JSON's default - UTF-8.
 * The JSON handler supports UTF-8, UTF-16 and UTF-32.
 *
 * To set the charset, the request must have content type specified as
 * "application/json; charset=UTF-8" (replace UTF-8 with UTF-16 or UTF-32 as
 * required).
 *
 * One way to create an event in the format expected by this handler, is to
 * use {@linkplain JSONEvent} and use {@linkplain Gson} to create the JSON
 * string using the
 * {@linkplain Gson#toJson(java.lang.Object, java.lang.reflect.Type) }
 * method. The type token to pass as the 2nd argument of this method
 * for list of events can be created by: <p>
 *
 * Type type = new TypeToken<List<JSONEvent>>() {}.getType(); <p>
 *
 */

public class JSONHandler implements HTTPSourceHandler {

  private static final Logger LOG = LoggerFactory.getLogger(JSONHandler.class);
  private final Type listType =
          new TypeToken<List<JSONEvent>>() {
          }.getType();
  private final Gson gson;

  public JSONHandler() {
    gson = new GsonBuilder().disableHtmlEscaping().create();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<Event> getEvents(HttpServletRequest request) throws Exception {
    BufferedReader reader = request.getReader();
    String charset = request.getCharacterEncoding();
    //UTF-8 is default for JSON. If no charset is specified, UTF-8 is to
    //be assumed.
    if (charset == null) {
      LOG.debug("Charset is null, default charset of UTF-8 will be used.");
      charset = "UTF-8";
    } else if (!(charset.equalsIgnoreCase("utf-8")
            || charset.equalsIgnoreCase("utf-16")
            || charset.equalsIgnoreCase("utf-32"))) {
      LOG.error("Unsupported character set in request {}. "
              + "JSON handler supports UTF-8, "
              + "UTF-16 and UTF-32 only.", charset);
      throw new UnsupportedCharsetException("JSON handler supports UTF-8, "
              + "UTF-16 and UTF-32 only.");
    }

    /*
     * Gson throws Exception if the data is not parseable to JSON.
     * Need not catch it since the source will catch it and return error.
     */
    List<Event> eventList = new ArrayList<Event>(0);
    try {
      eventList = gson.fromJson(reader, listType);
    } catch (JsonSyntaxException ex) {
      throw new HTTPBadRequestException("Request has invalid JSON Syntax.", ex);
    }

    for (Event e : eventList) {
      ((JSONEvent) e).setCharset(charset);
    }
    return getSimpleEvents(eventList);
  }

  @Override
  public void configure(Context context) {
  }

  private List<Event> getSimpleEvents(List<Event> events) {
    List<Event> newEvents = new ArrayList<Event>(events.size());
    for(Event e:events) {
      newEvents.add(EventBuilder.withBody(e.getBody(), e.getHeaders()));
    }
    return newEvents;
  }
}
