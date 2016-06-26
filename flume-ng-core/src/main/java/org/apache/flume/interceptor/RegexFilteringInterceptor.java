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

import static org.apache.flume.interceptor.RegexFilteringInterceptor.Constants.DEFAULT_EXCLUDE_EVENTS;
import static org.apache.flume.interceptor.RegexFilteringInterceptor.Constants.DEFAULT_REGEX;
import static org.apache.flume.interceptor.RegexFilteringInterceptor.Constants.EXCLUDE_EVENTS;
import static org.apache.flume.interceptor.RegexFilteringInterceptor.Constants.REGEX;

import java.util.List;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Interceptor that filters events selectively based on a configured regular
 * expression matching against the event body.
 *
 * This supports either include- or exclude-based filtering. A given
 * interceptor can only perform one of these functions, but multiple
 * interceptor can be chained together to create more complex
 * inclusion/exclusion patterns. If include-based filtering is configured, then
 * all events matching the supplied regular expression will be passed through
 * and all events not matching will be ignored. If exclude-based filtering is
 * configured, than all events matching will be ignored, and all other events
 * will pass through.
 *
 * Note that all regular expression matching occurs through Java's built in
 * java.util.regex package.
 *
 * Properties:<p>
 *
 *   regex: Regular expression for matching excluded events.
 *          (default is ".*")<p>
 *
 *   excludeEvents: If true, a regex match determines events to exclude,
 *                  otherwise a regex determines events to include
 *                  (default is false)<p>
 *
 * Sample config:<p>
 *
 * <code>
 *   agent.sources.r1.channels = c1<p>
 *   agent.sources.r1.type = SEQ<p>
 *   agent.sources.r1.interceptors = i1<p>
 *   agent.sources.r1.interceptors.i1.type = REGEX<p>
 *   agent.sources.r1.interceptors.i1.regex = (WARNING)|(ERROR)|(FATAL)<p>
 * </code>
 *
 */
public class RegexFilteringInterceptor implements Interceptor {

  private static final Logger logger = LoggerFactory
      .getLogger(StaticInterceptor.class);

  private final Pattern regex;
  private final boolean excludeEvents;

  /**
   * Only {@link RegexFilteringInterceptor.Builder} can build me
   */
  private RegexFilteringInterceptor(Pattern regex, boolean excludeEvents) {
    this.regex = regex;
    this.excludeEvents = excludeEvents;
  }

  @Override
  public void initialize() {
    // no-op
  }


  @Override
  /**
   * Returns the event if it passes the regular expression filter and null
   * otherwise.
   */
  public Event intercept(Event event) {
    // We've already ensured here that at most one of includeRegex and
    // excludeRegex are defined.

    if (!excludeEvents) {
      if (regex.matcher(new String(event.getBody())).find()) {
        return event;
      } else {
        return null;
      }
    } else {
      if (regex.matcher(new String(event.getBody())).find()) {
        return null;
      } else {
        return event;
      }
    }
  }

  /**
   * Returns the set of events which pass filters, according to
   * {@link #intercept(Event)}.
   * @param events
   * @return
   */
  @Override
  public List<Event> intercept(List<Event> events) {
    List<Event> out = Lists.newArrayList();
    for (Event event : events) {
      Event outEvent = intercept(event);
      if (outEvent != null) {
        out.add(outEvent);
      }
    }
    return out;
  }

  @Override
  public void close() {
    // no-op
  }

  /**
   * Builder which builds new instance of the StaticInterceptor.
   */
  public static class Builder implements Interceptor.Builder {

    private Pattern regex;
    private boolean excludeEvents;

    @Override
    public void configure(Context context) {
      String regexString = context.getString(REGEX, DEFAULT_REGEX);
      regex = Pattern.compile(regexString);
      excludeEvents = context.getBoolean(EXCLUDE_EVENTS,
          DEFAULT_EXCLUDE_EVENTS);
    }

    @Override
    public Interceptor build() {
      logger.info(String.format(
          "Creating RegexFilteringInterceptor: regex=%s,excludeEvents=%s",
          regex, excludeEvents));
      return new RegexFilteringInterceptor(regex, excludeEvents);
    }
  }

  public static class Constants {

    public static final String REGEX = "regex";
    public static final String DEFAULT_REGEX = ".*";

    public static final String EXCLUDE_EVENTS = "excludeEvents";
    public static final boolean DEFAULT_EXCLUDE_EVENTS = false;
  }

}