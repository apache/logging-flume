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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flume.interceptor.StaticInterceptor.Constants.*;

/**
 * Interceptor class that appends a static, pre-configured header to all events.
 *
 * Properties:<p>
 *
 *   key: Key to use in static header insertion.
 *        (default is "key")<p>
 *
 *   value: Value to use in static header insertion.
 *        (default is "value")<p>
 *
 *   preserveExisting: Whether to preserve an existing value for 'key'
 *                     (default is true)<p>
 *
 * Sample config:<p>
 *
 * <code>
 *   agent.sources.r1.channels = c1<p>
 *   agent.sources.r1.type = SEQ<p>
 *   agent.sources.r1.interceptors = i1<p>
 *   agent.sources.r1.interceptors.i1.type = static<p>
 *   agent.sources.r1.interceptors.i1.preserveExisting = false<p>
 *   agent.sources.r1.interceptors.i1.key = datacenter<p>
 *   agent.sources.r1.interceptors.i1.value= NYC_01<p>
 * </code>
 *
 */
public class StaticInterceptor implements Interceptor {

  private static final Logger logger = LoggerFactory
      .getLogger(StaticInterceptor.class);

  private final boolean preserveExisting;
  private final String key;
  private final String value;

  /**
   * Only {@link HostInterceptor.Builder} can build me
   */
  private StaticInterceptor(boolean preserveExisting, String key,
      String value) {
    this.preserveExisting = preserveExisting;
    this.key = key;
    this.value = value;
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

    if (preserveExisting && headers.containsKey(key)) {
      return event;
    }

    headers.put(key, value);
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
   * Builder which builds new instance of the StaticInterceptor.
   */
  public static class Builder implements Interceptor.Builder {

    private boolean preserveExisting;
    private String key;
    private String value;

    @Override
    public void configure(Context context) {
      preserveExisting = context.getBoolean(PRESERVE, PRESERVE_DEFAULT);
      key = context.getString(KEY, KEY_DEFAULT);
      value = context.getString(VALUE, VALUE_DEFAULT);
    }

    @Override
    public Interceptor build() {
      logger.info(String.format(
          "Creating StaticInterceptor: preserveExisting=%s,key=%s,value=%s",
          preserveExisting, key, value));
      return new StaticInterceptor(preserveExisting, key, value);
    }


  }

  public static class Constants {

    public static final String KEY = "key";
    public static final String KEY_DEFAULT = "key";

    public static final String VALUE = "value";
    public static final String VALUE_DEFAULT = "value";

    public static final String PRESERVE = "preserveExisting";
    public static final boolean PRESERVE_DEFAULT = true;
  }
}
