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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

/**
 * Interceptor that extracts matches using a specified regular expression and
 * appends the matches to the event headers using the specified serializers</p>
 * Note that all regular expression matching occurs through Java's built in
 * java.util.regex package</p>. Properties:
 * <p>
 * regex: The regex to use
 * <p>
 * serializers: Comma separated list of headerName:fully qualified serializer
 * class, if the serializer class is not specified the default
 * {@link RegexExtractorInterceptorPassThroughSerializer} will be used
 * <p>
 * Sample config:
 * <p>
 * <code>
 *   agent.sources.r1.channels = c1<p>
 *   agent.sources.r1.type = SEQ<p>
 *   agent.sources.r1.interceptors = i1<p>
 *   agent.sources.r1.interceptors.i1.type = REGEX_EXTRACTOR<p>
 *   agent.sources.r1.interceptors.i1.regex = (WARNING)|(ERROR)|(FATAL)<p>
 *   agent.sources.r1.interceptors.i1.serializer = warning:com.blah.SomeSerializer,error,fatal:org.apache.flume.interceptor.RegexExtractorInterceptorTimestampSerializer<p>
 *   agent.sources.r1.interceptors.i1.org.apache.flume.interceptor.RegexExtractorInterceptorTimestampSerializer.dateFormat = yyyy-MM-dd
 * </code>
 * </p>
 * <pre>
 * Example 1:
 * </p>
 * EventBody: 1:2:3.4foobar5</p> Configuration:
 * agent.sources.r1.interceptors.i1.regex = (\\d):(\\d):(\\d)
 * </p>
 * agent.sources.r1.interceptors.i1.matchGroupIds = one,two,three
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
 * <p>
 * agent.sources.r1.interceptors.i1.matchGroupIds = one,two
 * <p>
 *
 * results in an event with the the following
 *
 * body: 1:2:3.4foobar5 headers: one=>1, two=>2
 * </pre>
 */
public class RegexExtractorInterceptor implements Interceptor {

  static final String REGEX = "regex";
  static final String SERIALIZERS = "serializers";

  private static final Logger logger = LoggerFactory
      .getLogger(RegexExtractorInterceptor.class);

  private final Pattern regex;
  private final List<NameAndSerializer> serializers;

  private RegexExtractorInterceptor(Pattern regex,
      List<NameAndSerializer> serializers) {
    this.regex = regex;
    this.serializers = serializers;
  }

  @Override
  public void initialize() {
    // NO-OP...
  }

  @Override
  public void close() {
    // NO-OP...
  }

  @Override
  public Event intercept(Event event) {
    Matcher matcher = regex.matcher(new String(event.getBody()));
    Map<String, String> headers = event.getHeaders();
    if (matcher.find()) {
      for (int group = 0, count = matcher.groupCount(); group < count; group++) {
        int groupIndex = group + 1;
        if (groupIndex > serializers.size()) {
          if (logger.isDebugEnabled()) {
            logger.debug("Skipping group {} to {} due to missing serializer",
                group, count);
          }
          break;
        }
        NameAndSerializer serializer = serializers.get(group);
        if (logger.isDebugEnabled()) {
          logger.debug("Serializing {} using {}", serializer.headerName,
              serializer.serializer);
        }
        headers.put(serializer.headerName,
            serializer.serializer.serialize(matcher.group(groupIndex)));
      }
    }
    return event;
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    List<Event> intercepted = Lists.newArrayListWithCapacity(events.size());
    for (Event event : events) {
      Event interceptedEvent = intercept(event);
      if (interceptedEvent != null) {
        intercepted.add(interceptedEvent);
      }
    }
    return intercepted;
  }

  public static class Builder implements Interceptor.Builder {

    private Pattern regex;
    private List<NameAndSerializer> serializerList;
    private final RegexExtractorInterceptorSerializer defaultSerializer = new RegexExtractorInterceptorPassThroughSerializer();

    @Override
    public void configure(Context context) {
      String regexString = context.getString(REGEX);
      Preconditions.checkArgument(!StringUtils.isEmpty(regexString),
          "Must supply a valid regex string");
      regex = Pattern.compile(regexString);

      configureSerializers(context);
    }

    private void configureSerializers(Context context) {
      String serializerString = context.getString(SERIALIZERS);
      Preconditions.checkArgument(!StringUtils.isEmpty(serializerString),
          "Must supply at least one name and serializer");

      String[] nameAndSerializerList = serializerString.split(",");
      serializerList = Lists
          .newArrayListWithCapacity(nameAndSerializerList.length);
      for (String nameAndSerializer : nameAndSerializerList) {
        String[] splitNameAndSerializer = nameAndSerializer.split(":");
        String name = splitNameAndSerializer[0].trim();
        if (splitNameAndSerializer.length > 1) {
          String serializer = splitNameAndSerializer[1].trim();
          serializerList.add(new NameAndSerializer(name, getCustomSerializer(
              serializer, context)));
        } else {
          serializerList.add(new NameAndSerializer(name, defaultSerializer));
        }
      }
    }

    private RegexExtractorInterceptorSerializer getCustomSerializer(
        String clazzName, Context context) {
      try {
        Context serializerContext = new Context();
        serializerContext.putAll(context.getSubProperties(clazzName + "."));
        RegexExtractorInterceptorSerializer serializer = (RegexExtractorInterceptorSerializer) Class
            .forName(clazzName).newInstance();
        serializer.configure(serializerContext);
        return serializer;
      } catch (Exception e) {
        logger.error("Could not instantiate event serializer.", e);
        Throwables.propagate(e);
      }
      return defaultSerializer;
    }

    @Override
    public Interceptor build() {
      Preconditions.checkArgument(regex != null,
          "Regex pattern was misconfigured");
      Preconditions.checkArgument(serializerList.size() > 0,
          "Must supply a valid group match id list");
      return new RegexExtractorInterceptor(regex, serializerList);
    }
  }

  static class NameAndSerializer {
    private final String headerName;
    private final RegexExtractorInterceptorSerializer serializer;

    public NameAndSerializer(String headerName,
        RegexExtractorInterceptorSerializer serializer) {
      this.headerName = headerName;
      this.serializer = serializer;
    }
  }
}
