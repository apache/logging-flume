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
 * By convention, this timestamp header is named "timestamp" and its format
 * is a "stringified" long timestamp in milliseconds since the UNIX epoch.
 */
public class TimestampInterceptor implements Interceptor {

  private final boolean preserveExisting;

  /**
   * Only {@link TimestampInterceptor.Builder} can build me
   */
  private TimestampInterceptor(boolean preserveExisting) {
    this.preserveExisting = preserveExisting;
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
    if (preserveExisting && headers.containsKey(TIMESTAMP)) {
      // we must preserve the existing timestamp
    } else {
      long now = System.currentTimeMillis();
      headers.put(TIMESTAMP, Long.toString(now));
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

    private boolean preserveExisting = PRESERVE_DFLT;

    @Override
    public Interceptor build() {
      return new TimestampInterceptor(preserveExisting);
    }

    @Override
    public void configure(Context context) {
      preserveExisting = context.getBoolean(PRESERVE, PRESERVE_DFLT);
    }

  }

  public static class Constants {
    public static String TIMESTAMP = "timestamp";
    public static String PRESERVE = "preserveExisting";
    public static boolean PRESERVE_DFLT = false;
  }

}
