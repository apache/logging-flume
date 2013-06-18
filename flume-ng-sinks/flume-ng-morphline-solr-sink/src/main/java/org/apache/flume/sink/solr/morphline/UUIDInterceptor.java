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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

/**
 * Flume Interceptor that sets a universally unique identifier on all events
 * that are intercepted. By default this event header is named "id".
 */
public class UUIDInterceptor implements Interceptor {

  private String headerName;
  private boolean preserveExisting;
  private String prefix;

  public static final String HEADER_NAME = "headerName";
  public static final String PRESERVE_EXISTING_NAME = "preserveExisting";
  public static final String PREFIX_NAME = "prefix";

  protected UUIDInterceptor(Context context) {
    headerName = context.getString(HEADER_NAME, "id");
    preserveExisting = context.getBoolean(PRESERVE_EXISTING_NAME, true);
    prefix = context.getString(PREFIX_NAME, "");
  }

  @Override
  public void initialize() {
  }

  protected String getPrefix() {
    return prefix;
  }

  protected String generateUUID() {
    return getPrefix() + UUID.randomUUID().toString();
  }

  protected boolean isMatch(Event event) {
    return true;
  }

  @Override
  public Event intercept(Event event) {
    Map<String, String> headers = event.getHeaders();
    if (preserveExisting && headers.containsKey(headerName)) {
      // we must preserve the existing id
    } else if (isMatch(event)) {
      headers.put(headerName, generateUUID());
    }
    return event;
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    List results = new ArrayList(events.size());
    for (Event event : events) {
      event = intercept(event);
      if (event != null) {
        results.add(event);
      }
    }
    return results;
  }

  @Override
  public void close() {
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  /** Builder implementations MUST have a public no-arg constructor */
  public static class Builder implements Interceptor.Builder {

    private Context context;

    public Builder() {
    }

    @Override
    public UUIDInterceptor build() {
      return new UUIDInterceptor(context);
    }

    @Override
    public void configure(Context context) {
      this.context = context;
    }

  }

}
