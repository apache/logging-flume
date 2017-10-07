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

import java.util.List;
import java.util.Map;
import org.apache.flume.Context;
import org.apache.flume.Event;

import static org.apache.flume.interceptor.TimestampInterceptor.Constants.*;

/**
 * Simple Interceptor class that sets the current system timestamp on all events
 * that are intercepted.
 * By convention, this timestamp header is named "timestamp" by default and its format
 * is a "stringified" long timestamp in milliseconds since the UNIX epoch.
 * The name of the header can be changed through the configuration using the
 * config key "header".
 */
public class TimestampInterceptor implements Interceptor {

  private final boolean preserveExisting;
  private final String header;

  /**
   * Only {@link TimestampInterceptor.Builder} can build me
   */
  private TimestampInterceptor(boolean preserveExisting, String header) {
    this.preserveExisting = preserveExisting;
    this.header = header;
  }

  @Override
  public void initialize() {
    // no-op
  }

  /**
   * Modifies events in-place.
   */
  @Override
  public Event intercept(Event event) {
    Map<String, String> headers = event.getHeaders();
    if (preserveExisting && headers.containsKey(header)) {
      // we must preserve the existing timestamp
    } else {
      long now = System.currentTimeMillis();
      headers.put(header, Long.toString(now));
    }
    return event;
  }

  /**
   * Delegates to {@link #intercept(Event)} in a loop.
   * @param events
   * @return
   */
  @Override
  public List<Event> intercept(List<Event> events) {
    for (Event event : events) {
      intercept(event);
    }
    return events;
  }

  @Override
  public void close() {
    // no-op
  }

  /**
   * Builder which builds new instances of the TimestampInterceptor.
   */
  public static class Builder implements Interceptor.Builder {

    private boolean preserveExisting = DEFAULT_PRESERVE;
    private String header = DEFAULT_HEADER_NAME;

    @Override
    public Interceptor build() {
      return new TimestampInterceptor(preserveExisting, header);
    }

    @Override
    public void configure(Context context) {
      preserveExisting = context.getBoolean(CONFIG_PRESERVE, DEFAULT_PRESERVE);
      header = context.getString(CONFIG_HEADER_NAME, DEFAULT_HEADER_NAME);
    }

  }

  public static class Constants {
    public static final String CONFIG_PRESERVE = "preserveExisting";
    public static final boolean DEFAULT_PRESERVE = false;
    public static final String CONFIG_HEADER_NAME = "headerName";
    public static final String DEFAULT_HEADER_NAME = "timestamp";
  }

}
