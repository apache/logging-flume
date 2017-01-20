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

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.LogPrivacyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This interceptor manipulates Flume event headers, by removing one or many
 * headers. It can remove a statically defined header, headers based on a
 * regular expression or headers in a list. If none of these is defined, or if
 * no header matches the criteria, the Flume events are not modified.<br />
 * Note that if only one header needs to be removed, specifying it by name
 * provides performance benefits over the other two methods.<br />
 * <br />
 * Properties:<br />
 * - .withName (optional): name of the header to remove<br />
 * - .fromList (optional): list of headers to remove, separated with the
 * separator specified with .from.list.separator<br />
 * - .fromListSeparator (optional): regular expression used to separate
 * multiple header names in the list specified using .from.list<br />
 * - .matching (optional): All the headers which names match this regular expression are
 * removed
 */
public class RemoveHeaderInterceptor implements Interceptor {
  static final String WITH_NAME = "withName";
  static final String FROM_LIST = "fromList";
  static final String LIST_SEPARATOR = "fromListSeparator";
  static final String LIST_SEPARATOR_DEFAULT = "\\s*,\\s*";
  static final String MATCH_REGEX = "matching";
  private static final Logger LOG = LoggerFactory
      .getLogger(RemoveHeaderInterceptor.class);
  private final String withName;
  private final Set<String> fromList;
  private final Pattern matchRegex;

  /**
   * Only {@link RemoveHeaderInterceptor.Builder} can build me
   */
  private RemoveHeaderInterceptor(final String withName, final String fromList,
                                  final String listSeparator, final Pattern matchRegex) {
    this.withName = withName;
    assert listSeparator != null : "Default value used otherwise";
    this.fromList = (fromList != null) ? new HashSet<>(Arrays.asList(fromList.split(
            listSeparator))) : null;
    this.matchRegex = matchRegex;
  }

  /**
   * @see org.apache.flume.interceptor.Interceptor#initialize()
   */
  @Override
  public void initialize() {
    // Nothing to do
  }

  /**
   * @see org.apache.flume.interceptor.Interceptor#close()
   */
  @Override
  public void close() {
    // Nothing to do
  }

  /**
   * @see org.apache.flume.interceptor.Interceptor#intercept(java.util.List)
   */
  @Override
  public List<Event> intercept(final List<Event> events) {
    for (final Event event : events) {
      intercept(event);
    }
    return events;
  }

  /**
   * @see org.apache.flume.interceptor.Interceptor#intercept(org.apache.flume.Event)
   */
  @Override
  public Event intercept(final Event event) {
    assert event != null : "Missing Flume event while intercepting";
    try {
      final Map<String, String> headers = event.getHeaders();
      // If withName matches, removing it directly
      if (withName != null && headers.remove(withName) != null) {
        LOG.trace("Removed header \"{}\" for event: {}", withName, event);
      }
      // Also, we need to go through the list
      if (fromList != null || matchRegex != null) {
        final Iterator<String> headerIterator = headers.keySet().iterator();
        List<String> removedHeaders = new LinkedList<>();
        while (headerIterator.hasNext()) {
          final String currentHeader = headerIterator.next();
          if (fromList != null && fromList.contains(currentHeader)) {
            headerIterator.remove();
            removedHeaders.add(currentHeader);
          } else if (matchRegex != null) {
            final Matcher matcher = matchRegex.matcher(currentHeader);
            if (matcher.matches()) {
              headerIterator.remove();
              removedHeaders.add(currentHeader);
            }
          }
        }
        if (!removedHeaders.isEmpty() && LogPrivacyUtil.allowLogRawData()) {
          LOG.trace("Removed headers \"{}\" for event: {}", removedHeaders, event);
        }
      }
    } catch (final Exception e) {
      LOG.error("Failed to process event " + event, e);
    }
    return event;
  }

  /**
   * Builder which builds new instances of the {@link RemoveHeaderInterceptor}.
   */
  public static class Builder implements Interceptor.Builder {
    String withName;
    String fromList;
    String listSeparator;
    Pattern matchRegex;

    /**
     * @see org.apache.flume.interceptor.Interceptor.Builder#build()
     */
    @Override
    public Interceptor build() {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Creating RemoveHeaderInterceptor with: withName={}, fromList={}, " +
            "listSeparator={}, matchRegex={}", new String[] {withName, fromList, listSeparator,
                String.valueOf(matchRegex)});
      }
      return new RemoveHeaderInterceptor(withName, fromList, listSeparator,
          matchRegex);
    }

    /**
     * @see org.apache.flume.conf.Configurable#configure(org.apache.flume.Context)
     */
    @Override
    public void configure(final Context context) {
      withName = context.getString(WITH_NAME);
      fromList = context.getString(FROM_LIST);
      listSeparator = context.getString(LIST_SEPARATOR,
          LIST_SEPARATOR_DEFAULT);
      final String matchRegexStr = context.getString(MATCH_REGEX);
      if (matchRegexStr != null) {
        matchRegex = Pattern.compile(matchRegexStr);
      }
    }
  }
}
