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

package org.apache.flume.interceptor;

import java.util.List;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;

/**
 * Simple interceptor that removes a header called "Bad-Words" from all events.
 */
public class CensoringInterceptor implements Interceptor {

  private CensoringInterceptor() {
    // no-op
  }

  @Override
  public void initialize() {
    // no-op
  }

  @Override
  public Event intercept(Event event) {
    Map<String, String> headers = event.getHeaders();
    if (headers.containsKey("Bad-Words")) {
      headers.remove("Bad-Words");
    }
    return event;
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    for (Event e : events) {
      intercept(e);
    }
    return events;
  }

  @Override
  public void close() {
    // no-op
  }

  public static class Builder implements Interceptor.Builder {

    @Override
    public Interceptor build() {
      return new CensoringInterceptor();
    }

    @Override
    public void configure(Context context) {
      // no-op
    }

  }

}
