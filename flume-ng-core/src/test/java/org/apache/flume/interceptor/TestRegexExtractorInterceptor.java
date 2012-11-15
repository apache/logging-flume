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

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.Interceptor.Builder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TestRegexExtractorInterceptor {

  private Builder fixtureBuilder;

  @Before
  public void init() throws Exception {
    fixtureBuilder = InterceptorBuilderFactory
        .newInstance(InterceptorType.REGEX_EXTRACTOR.toString());
  }

  @Test
  public void shouldNotAllowConfigurationWithoutRegex() throws Exception {
    try {
      fixtureBuilder.build();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Pass...
    }
  }

  @Test
  public void shouldNotAllowConfigurationWithIllegalRegex() throws Exception {
    try {
      Context context = new Context();
      context.put(RegexExtractorInterceptor.REGEX, "?&?&&&?&?&?&&&??");
      fixtureBuilder.configure(context);
      fixtureBuilder.build();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Pass...
    }
  }

  @Test
  public void shouldNotAllowConfigurationWithoutMatchIds() throws Exception {
    try {
      Context context = new Context();
      context.put(RegexExtractorInterceptor.REGEX, ".*");
      context.put(RegexExtractorInterceptor.SERIALIZERS, "");
      fixtureBuilder.configure(context);
      fixtureBuilder.build();
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Pass...
    }
  }

  @Test
  public void shouldExtractAddHeadersForAllMatchGroups() throws Exception {
    Context context = new Context();
    context.put(RegexExtractorInterceptor.REGEX, "(\\d):(\\d):(\\d)");
    context.put(RegexExtractorInterceptor.SERIALIZERS, "Num1,Num2,Num3");
    fixtureBuilder.configure(context);
    Interceptor fixture = fixtureBuilder.build();

    Event event = EventBuilder.withBody("1:2:3.4foobar5", Charsets.UTF_8);

    Event expected = EventBuilder.withBody("1:2:3.4foobar5", Charsets.UTF_8);
    expected.getHeaders().put("Num1", "1");
    expected.getHeaders().put("Num2", "2");
    expected.getHeaders().put("Num3", "3");

    Event actual = fixture.intercept(event);

    Assert.assertArrayEquals(expected.getBody(), actual.getBody());
    Assert.assertEquals(expected.getHeaders(), actual.getHeaders());
  }

  @Test
  public void shouldExtractAddHeadersForAllMatchGroupsIgnoringMissingIds()
      throws Exception {
    String body = "2012-10-17 14:34:44,338";
    Context context = new Context();
    // Skip the second group
    context.put(RegexExtractorInterceptor.REGEX,
        "^(\\d\\d\\d\\d-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d)(:\\d\\d,\\d\\d\\d)");
    context.put(RegexExtractorInterceptor.SERIALIZERS, "timestamp");

    fixtureBuilder.configure(context);
    Interceptor fixture = fixtureBuilder.build();

    Event event = EventBuilder.withBody(body, Charsets.UTF_8);
    Event expected = EventBuilder.withBody(body, Charsets.UTF_8);
    expected.getHeaders().put("timestamp", "2012-10-17 14:34");

    Event actual = fixture.intercept(event);

    Assert.assertArrayEquals(expected.getBody(), actual.getBody());
    Assert.assertEquals(expected.getHeaders(), actual.getHeaders());

  }

  @Test
  public void shouldExtractAddHeadersUsingSpecifiedSerializer()
      throws Exception {
    String body = "2012-10-17 14:34:44,338";
    Context context = new Context();
    // Skip the second group
    context.put(RegexExtractorInterceptor.REGEX,
        "^(\\d\\d\\d\\d-\\d\\d-\\d\\d\\s\\d\\d:\\d\\d)(:\\d\\d,\\d\\d\\d)");
    context.put(RegexExtractorInterceptor.SERIALIZERS, "timestamp:"
        + RegexExtractorInterceptorMillisSerializer.class.getName() + ",data");
    context.put(RegexExtractorInterceptorMillisSerializer.class.getName()
        + ".pattern", "yyyy-MM-dd HH:mm");

    fixtureBuilder.configure(context);
    Interceptor fixture = fixtureBuilder.build();

    Event event = EventBuilder.withBody(body, Charsets.UTF_8);
    Event expected = EventBuilder.withBody(body, Charsets.UTF_8);
    expected.getHeaders().put("timestamp", "1350509640000");
    expected.getHeaders().put("data", ":44,338");

    Event actual = fixture.intercept(event);

    Assert.assertArrayEquals(expected.getBody(), actual.getBody());
    Assert.assertEquals(expected.getHeaders(), actual.getHeaders());
  }
}
