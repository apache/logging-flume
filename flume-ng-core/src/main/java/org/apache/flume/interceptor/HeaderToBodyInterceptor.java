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

package org.apache.flume.interceptor;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interceptor that extracts matches using a specified regular expression and
 * appends the matches to the event headers using the specified serializers</p>
 * Note that all regular expression matching occurs through Java's built in
 * java.util.regex package</p>. Properties:
 * regex: The regex to use
 * serializers: Specifies the group the serializer will be applied to, and the
 * name of the header that will be added. If no serializer is specified for a
 * group the default {@link RegexExtractorInterceptorPassThroughSerializer} will
 * be used
 * Sample config:
 * agent.sources.r1.channels = c1
 * agent.sources.r1.type = SEQ
 * agent.sources.r1.interceptors = i1
 * agent.sources.r1.interceptors.i1.type = REGEX_EXTRACTOR
 * agent.sources.r1.interceptors.i1.regex = (WARNING)|(ERROR)|(FATAL)
 * <p>
 * agent.sources.r1.interceptors.i1.serializers = s1 s2
 * agent.sources.r1.interceptors.i1.serializers.s1.type = com.blah.SomeSerializer
 * agent.sources.r1.interceptors.i1.serializers.s1.name = warning
 * agent.sources.r1.interceptors.i1.serializers.s2.type =
 *     org.apache.flume.interceptor.RegexExtractorInterceptorTimestampSerializer
 * agent.sources.r1.interceptors.i1.serializers.s2.name = error
 * agent.sources.r1.interceptors.i1.serializers.s2.dateFormat = yyyy-MM-dd
 * </p>
 * <pre>
 * Example 1:
 * </p>
 * EventBody: 1:2:3.4foobar5</p> Configuration:
 * agent.sources.r1.interceptors.i1.regex = (\\d):(\\d):(\\d)
 * </p>
 * agent.sources.r1.interceptors.i1.serializers = s1 s2 s3
 * agent.sources.r1.interceptors.i1.serializers.s1.name = one
 * agent.sources.r1.interceptors.i1.serializers.s2.name = two
 * agent.sources.r1.interceptors.i1.serializers.s3.name = three
 * </p>
 * results in an event with the the following
 *
 * body: 1:2:3.4foobar5 headers: one=>1, two=>2, three=3
 *
 * Example 2:
 *
 * EventBody: 1:2:3.4foobar5
 *
 * Configuration: agent.sources.r1.interceptors.i1.regex = (\\d):(\\d):(\\d)
 * agent.sources.r1.interceptors.i1.serializers = s1 s2
 * agent.sources.r1.interceptors.i1.serializers.s1.name = one
 * agent.sources.r1.interceptors.i1.serializers.s2.name = two
 *
 * results in an event with the the following
 *
 * body: 1:2:3.4foobar5 headers: one=>1, two=>2
 * </pre>
 */
public class HeaderToBodyInterceptor implements Interceptor {

  private static final Logger log = LoggerFactory.getLogger(HeaderToBodyInterceptor.class);
  private static Gson gson = new Gson();

  String bodyType = null;
  List<String[]> headerToBodyList = null;

  public HeaderToBodyInterceptor(String bodyType, List<String[]> headerToBodyList) {
    this.bodyType = bodyType;
    this.headerToBodyList = headerToBodyList;
  }

  @Override
  public void initialize() {
    // TODO Auto-generated method stub

  }

  @Override
  public List<Event> intercept(List<Event> events) {
    for (final Event event : events) {
      intercept(event);
    }
    return events;
  }

  @Override
  public Event intercept(Event event) {

    if (this.bodyType != null && !this.bodyType.isEmpty()) {
      byte[] newBody = null;
      
      if (log.isDebugEnabled()) {
        log.debug("bodyType : " + this.bodyType);
        log.debug("headerToBody : " + this.headerToBodyList.toString());
        log.debug("old event : " + event.toString());
      }
      
      if (this.bodyType.compareToIgnoreCase("json") == 0) {
        newBody = headerToBodyJson(event.getHeaders(), event.getBody());
      } else if (this.bodyType.compareToIgnoreCase("csv") == 0) {
        newBody = headerToBodyCsv(event.getHeaders(), event.getBody());
      } else {
        newBody = event.getBody();
      }
      
      event.setBody(newBody);
      
      if (log.isDebugEnabled()) {
        log.debug("new event : " + event.toString());
      }
    }

    return event;
  }

  private byte[] headerToBodyCsv(Map<String, String> headers, byte[] body) {
    StringBuilder payload = new StringBuilder(body.toString());

    for (String[] item : this.headerToBodyList) {
      String itemHeader = item[0];
      String itemType = item[1];
      // String itemBody = item[2];

      if (itemType.isEmpty() || itemType == "string") {
        payload.append("," + headers.get(itemHeader));
      } else if (itemType == "int") {
        payload.append("," + Integer.parseInt(headers.get(itemHeader)));
      } else if (itemType == "long") {
        payload.append("," + Long.parseLong(headers.get(itemHeader)));
      } else if (itemType == "float") {
        payload.append("," + Float.parseFloat(headers.get(itemHeader)));
      } else if (itemType == "double") {
        payload.append("," + Double.parseDouble(headers.get(itemHeader)));
      } else {
        payload.append("," + headers.get(itemHeader));
      }
    }

    return payload.toString().getBytes();
  }

  private byte[] headerToBodyJson(Map<String, String> headers, byte[] body) {

    @SuppressWarnings("unchecked")
    Map<String, Object> payloadMap = gson.fromJson(new String(body), Map.class);

    for (String[] item : this.headerToBodyList) {
      String itemHeader = item[0];
      String itemType = item[1];
      String itemBody = item[2];

      if (itemType.isEmpty() || itemType == "string") {
        payloadMap.put(itemBody, headers.get(itemHeader));
      } else if (itemType == "int") {
        payloadMap.put(itemBody, Integer.parseInt(headers.get(itemHeader)));
      } else if (itemType == "long") {
        payloadMap.put(itemBody, Long.parseLong(headers.get(itemHeader)));
      } else if (itemType == "float") {
        payloadMap.put(itemBody, Float.parseFloat(headers.get(itemHeader)));
      } else if (itemType == "double") {
        payloadMap.put(itemBody, Double.parseDouble(headers.get(itemHeader)));
      } else {
        payloadMap.put(itemBody, headers.get(itemHeader));
      }
    }

    return gson.toJson(payloadMap).getBytes();
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub

  }

  /**
   * Builder which builds new instances of the {@link RemoveHeaderInterceptor}.
   */
  public static class Builder implements Interceptor.Builder {
    private String bodyType = null;
    private List<String[]> headerToBodyList = new ArrayList<>();
    static final String HEADER_TO_BODY = "headerToBody";
    static final String BODY_TYPE = "bodyType";

    @Override
    public Interceptor build() {
      if (log.isDebugEnabled()) {
        log.debug("Creating HeaderToBodyInterceptor with: " + headerToBodyList.toString());
      }
      return new HeaderToBodyInterceptor(bodyType, headerToBodyList);
    }

    @Override
    public void configure(final Context context) {
      bodyType = context.getString(BODY_TYPE);

      if (bodyType != null && !bodyType.isEmpty()) {
        Preconditions.checkArgument((bodyType.compareToIgnoreCase("json") != 0)
            || (bodyType.compareToIgnoreCase("csv") != 0), "body type isn't json or csv");
        String[] headerToBodys = context.getString(HEADER_TO_BODY).split(",");
        for (String item : headerToBodys) {
          String[] headerToBody = item.split(":");
          Preconditions.checkArgument(headerToBody.length != 2,
              "Must supply a valid  string, like headerTopic:string:bodyTopic");
          headerToBodyList.add(headerToBody);
        }
      }
    }
  }
}
