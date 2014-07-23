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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 * Interceptor that allows search-and-replace of event body strings using
 * regular expressions. This only works with event bodies that are valid
 * strings. The charset is configurable.
 * <p>
 * Usage:
 * <pre>
 *   agent.source-1.interceptors.search-replace.searchPattern = ^INFO:
 *   agent.source-1.interceptors.search-replace.replaceString = Log msg:
 * </pre>
 * <p>
 * Any regular expression search pattern and replacement pattern that can be
 * used with {@link java.util.regex.Matcher#replaceAll(String)} may be used,
 * including backtracking and grouping.
 */
public class SearchAndReplaceInterceptor implements Interceptor {

  private static final Logger logger = LoggerFactory
      .getLogger(SearchAndReplaceInterceptor.class);

  private final Pattern searchPattern;
  private final String replaceString;
  private final Charset charset;

  private SearchAndReplaceInterceptor(Pattern searchPattern,
    String replaceString,
    Charset charset) {
    this.searchPattern = searchPattern;
    this.replaceString = replaceString;
    this.charset = charset;
  }

  @Override
  public void initialize() {
  }

  @Override
  public void close() {
  }

  @Override
  public Event intercept(Event event) {
    String origBody = new String(event.getBody(), charset);
    Matcher matcher = searchPattern.matcher(origBody);
    String newBody = matcher.replaceAll(replaceString);
    event.setBody(newBody.getBytes(charset));
    return event;
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    for (Event event : events) {
      intercept(event);
    }
    return events;
  }

  public static class Builder implements Interceptor.Builder {
    private static final String SEARCH_PAT_KEY = "searchPattern";
    private static final String REPLACE_STRING_KEY = "replaceString";
    private static final String CHARSET_KEY = "charset";

    private Pattern searchRegex;
    private String replaceString;
    private Charset charset = Charsets.UTF_8;

    @Override
    public void configure(Context context) {
      String searchPattern = context.getString(SEARCH_PAT_KEY);
      Preconditions.checkArgument(!StringUtils.isEmpty(searchPattern),
          "Must supply a valid search pattern " + SEARCH_PAT_KEY +
          " (may not be empty)");

      replaceString = context.getString(REPLACE_STRING_KEY);
      Preconditions.checkNotNull(replaceString,
          "Must supply a replacement string " + REPLACE_STRING_KEY +
          " (empty is ok)");

      searchRegex = Pattern.compile(searchPattern);

      if (context.containsKey(CHARSET_KEY)) {
        // May throw IllegalArgumentException for unsupported charsets.
        charset = Charset.forName(context.getString(CHARSET_KEY));
      }
    }

    @Override
    public Interceptor build() {
      Preconditions.checkNotNull(searchRegex,
                                 "Regular expression search pattern required");
      Preconditions.checkNotNull(replaceString,
                                 "Replacement string required");
      return new SearchAndReplaceInterceptor(searchRegex, replaceString, charset);
    }
  }
}
