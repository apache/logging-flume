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

import com.google.common.collect.ImmutableMap;
import junit.framework.Assert;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

public class RemoveHeaderInterceptorTest {
  private static final String HEADER1 = "my-header10";
  private static final String HEADER2 = "my-header11";
  private static final String HEADER3 = "my-header12";
  private static final String HEADER4 = "my-header20";
  private static final String HEADER5 = "my-header21";
  private static final String DEFAULT_SEPARATOR = ", ";
  private static final String MY_SEPARATOR = ";";

  private Event buildEventWithHeader() {
    return EventBuilder.withBody("My test event".getBytes(), ImmutableMap.of(
        HEADER1, HEADER1, HEADER2, HEADER2, HEADER3, HEADER3, HEADER4, HEADER4,
        HEADER5, HEADER5));
  }

  private Event buildEventWithoutHeader() {
    return EventBuilder.withBody("My test event".getBytes());
  }

  @Test(expected = PatternSyntaxException.class)
  public void testBadConfig() throws Exception {
    new RemoveHeaderIntBuilder().fromList(HEADER1, "(").build();
  }

  @Test
  public void testWithName() throws IllegalAccessException, ClassNotFoundException,
      InstantiationException {
    final Interceptor removeHeaderInterceptor = new RemoveHeaderIntBuilder()
        .withName(HEADER4).build();
    final Event event1 = buildEventWithHeader();
    Assert.assertEquals(HEADER1, event1.getHeaders().get(HEADER1));
    Assert.assertEquals(HEADER2, event1.getHeaders().get(HEADER2));
    Assert.assertEquals(HEADER3, event1.getHeaders().get(HEADER3));
    Assert.assertEquals(HEADER4, event1.getHeaders().get(HEADER4));
    Assert.assertEquals(HEADER5, event1.getHeaders().get(HEADER5));
    removeHeaderInterceptor.intercept(event1);
    Assert.assertEquals(HEADER1, event1.getHeaders().get(HEADER1));
    Assert.assertEquals(HEADER2, event1.getHeaders().get(HEADER2));
    Assert.assertEquals(HEADER3, event1.getHeaders().get(HEADER3));
    Assert.assertNull(event1.getHeaders().get(HEADER4));
    Assert.assertEquals(HEADER5, event1.getHeaders().get(HEADER5));

    final Event event2 = buildEventWithoutHeader();
    Assert.assertTrue(event2.getHeaders().isEmpty());
    removeHeaderInterceptor.intercept(event2);
    Assert.assertTrue(event2.getHeaders().isEmpty());
  }

  @Test
  public void testFromListWithDefaultSeparator1() throws Exception {
    final Interceptor removeHeaderInterceptor = new RemoveHeaderIntBuilder()
        .fromList(HEADER4 + MY_SEPARATOR + HEADER2).build();
    final Event event1 = buildEventWithHeader();
    Assert.assertEquals(HEADER1, event1.getHeaders().get(HEADER1));
    Assert.assertEquals(HEADER2, event1.getHeaders().get(HEADER2));
    Assert.assertEquals(HEADER3, event1.getHeaders().get(HEADER3));
    Assert.assertEquals(HEADER4, event1.getHeaders().get(HEADER4));
    Assert.assertEquals(HEADER5, event1.getHeaders().get(HEADER5));
    removeHeaderInterceptor.intercept(event1);
    Assert.assertEquals(HEADER1, event1.getHeaders().get(HEADER1));
    Assert.assertEquals(HEADER2, event1.getHeaders().get(HEADER2));
    Assert.assertEquals(HEADER3, event1.getHeaders().get(HEADER3));
    Assert.assertEquals(HEADER4, event1.getHeaders().get(HEADER4));
    Assert.assertEquals(HEADER5, event1.getHeaders().get(HEADER5));

    final Event event2 = buildEventWithoutHeader();
    Assert.assertTrue(event2.getHeaders().isEmpty());
    removeHeaderInterceptor.intercept(event2);
    Assert.assertTrue(event2.getHeaders().isEmpty());
  }

  @Test
  public void testFromListWithDefaultSeparator2() throws Exception {
    final Interceptor removeHeaderInterceptor = new RemoveHeaderIntBuilder()
        .fromList(HEADER4 + DEFAULT_SEPARATOR + HEADER2).build();
    final Event event1 = buildEventWithHeader();
    Assert.assertEquals(HEADER1, event1.getHeaders().get(HEADER1));
    Assert.assertEquals(HEADER2, event1.getHeaders().get(HEADER2));
    Assert.assertEquals(HEADER3, event1.getHeaders().get(HEADER3));
    Assert.assertEquals(HEADER4, event1.getHeaders().get(HEADER4));
    Assert.assertEquals(HEADER5, event1.getHeaders().get(HEADER5));
    removeHeaderInterceptor.intercept(event1);
    Assert.assertEquals(HEADER1, event1.getHeaders().get(HEADER1));
    Assert.assertNull(event1.getHeaders().get(HEADER2));
    Assert.assertEquals(HEADER3, event1.getHeaders().get(HEADER3));
    Assert.assertNull(event1.getHeaders().get(HEADER4));
    Assert.assertEquals(HEADER5, event1.getHeaders().get(HEADER5));

    final Event event2 = buildEventWithoutHeader();
    Assert.assertTrue(event2.getHeaders().isEmpty());
    removeHeaderInterceptor.intercept(event2);
    Assert.assertTrue(event2.getHeaders().isEmpty());
  }

  @Test
  public void testFromListWithCustomSeparator1() throws Exception {
    final Interceptor removeHeaderInterceptor = new RemoveHeaderIntBuilder()
        .fromList(HEADER4 + MY_SEPARATOR + HEADER2, MY_SEPARATOR).build();
    final Event event1 = buildEventWithHeader();
    Assert.assertEquals(HEADER1, event1.getHeaders().get(HEADER1));
    Assert.assertEquals(HEADER2, event1.getHeaders().get(HEADER2));
    Assert.assertEquals(HEADER3, event1.getHeaders().get(HEADER3));
    Assert.assertEquals(HEADER4, event1.getHeaders().get(HEADER4));
    Assert.assertEquals(HEADER5, event1.getHeaders().get(HEADER5));
    removeHeaderInterceptor.intercept(event1);
    Assert.assertEquals(HEADER1, event1.getHeaders().get(HEADER1));
    Assert.assertNull(event1.getHeaders().get(HEADER2));
    Assert.assertEquals(HEADER3, event1.getHeaders().get(HEADER3));
    Assert.assertNull(event1.getHeaders().get(HEADER4));
    Assert.assertEquals(HEADER5, event1.getHeaders().get(HEADER5));

    final Event event2 = buildEventWithoutHeader();
    Assert.assertTrue(event2.getHeaders().isEmpty());
    removeHeaderInterceptor.intercept(event2);
    Assert.assertTrue(event2.getHeaders().isEmpty());
  }

  @Test
  public void testFromListWithCustomSeparator2() throws Exception {
    final Interceptor removeHeaderInterceptor = new RemoveHeaderIntBuilder()
        .fromList(HEADER4 + DEFAULT_SEPARATOR + HEADER2, MY_SEPARATOR).build();
    final Event event1 = buildEventWithHeader();
    Assert.assertEquals(HEADER1, event1.getHeaders().get(HEADER1));
    Assert.assertEquals(HEADER2, event1.getHeaders().get(HEADER2));
    Assert.assertEquals(HEADER3, event1.getHeaders().get(HEADER3));
    Assert.assertEquals(HEADER4, event1.getHeaders().get(HEADER4));
    Assert.assertEquals(HEADER5, event1.getHeaders().get(HEADER5));
    removeHeaderInterceptor.intercept(event1);
    Assert.assertEquals(HEADER1, event1.getHeaders().get(HEADER1));
    Assert.assertEquals(HEADER2, event1.getHeaders().get(HEADER2));
    Assert.assertEquals(HEADER3, event1.getHeaders().get(HEADER3));
    Assert.assertEquals(HEADER4, event1.getHeaders().get(HEADER4));
    Assert.assertEquals(HEADER5, event1.getHeaders().get(HEADER5));

    final Event event2 = buildEventWithoutHeader();
    Assert.assertTrue(event2.getHeaders().isEmpty());
    removeHeaderInterceptor.intercept(event2);
    Assert.assertTrue(event2.getHeaders().isEmpty());
  }

  @Test
  public void testMatchRegex() throws Exception {
    final Interceptor removeHeaderInterceptor = new RemoveHeaderIntBuilder()
        .matchRegex("my-header1.*").build();
    final Event event1 = buildEventWithHeader();
    Assert.assertEquals(HEADER1, event1.getHeaders().get(HEADER1));
    Assert.assertEquals(HEADER2, event1.getHeaders().get(HEADER2));
    Assert.assertEquals(HEADER3, event1.getHeaders().get(HEADER3));
    Assert.assertEquals(HEADER4, event1.getHeaders().get(HEADER4));
    Assert.assertEquals(HEADER5, event1.getHeaders().get(HEADER5));
    removeHeaderInterceptor.intercept(event1);
    Assert.assertNull(event1.getHeaders().get(HEADER1));
    Assert.assertNull(event1.getHeaders().get(HEADER2));
    Assert.assertNull(event1.getHeaders().get(HEADER3));
    Assert.assertEquals(HEADER4, event1.getHeaders().get(HEADER4));
    Assert.assertEquals(HEADER5, event1.getHeaders().get(HEADER5));

    final Event event2 = buildEventWithoutHeader();
    Assert.assertTrue(event2.getHeaders().isEmpty());
    removeHeaderInterceptor.intercept(event2);
    Assert.assertTrue(event2.getHeaders().isEmpty());
  }

  @Test
  public void testAll() throws Exception {
    final Interceptor removeHeaderInterceptor = new RemoveHeaderIntBuilder()
        .matchRegex("my-header2.*")
        .fromList(HEADER1 + MY_SEPARATOR + HEADER3, MY_SEPARATOR)
        .withName(HEADER2).build();
    final Event event1 = buildEventWithHeader();
    Assert.assertEquals(HEADER1, event1.getHeaders().get(HEADER1));
    Assert.assertEquals(HEADER2, event1.getHeaders().get(HEADER2));
    Assert.assertEquals(HEADER3, event1.getHeaders().get(HEADER3));
    Assert.assertEquals(HEADER4, event1.getHeaders().get(HEADER4));
    Assert.assertEquals(HEADER5, event1.getHeaders().get(HEADER5));
    removeHeaderInterceptor.intercept(event1);
    Assert.assertTrue(event1.getHeaders().isEmpty());

    final Event event2 = buildEventWithoutHeader();
    Assert.assertTrue(event2.getHeaders().isEmpty());
    removeHeaderInterceptor.intercept(event2);
    Assert.assertTrue(event2.getHeaders().isEmpty());
  }

  private static class RemoveHeaderIntBuilder {
    final Map<String, String> contextMap = new HashMap<>();

    RemoveHeaderIntBuilder withName(final String str) {
      contextMap.put(RemoveHeaderInterceptor.WITH_NAME, str);
      return this;
    }

    RemoveHeaderIntBuilder fromList(final String str) {
      contextMap.put(RemoveHeaderInterceptor.FROM_LIST, str);
      return this;
    }

    RemoveHeaderIntBuilder fromList(final String str,
                                    final String separator) {
      fromList(str);
      contextMap.put(RemoveHeaderInterceptor.LIST_SEPARATOR, separator);
      return this;
    }

    RemoveHeaderIntBuilder matchRegex(final String str) {
      contextMap.put(RemoveHeaderInterceptor.MATCH_REGEX, str);
      return this;
    }

    public Interceptor build() throws InstantiationException, IllegalAccessException,
        ClassNotFoundException {
      Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
          InterceptorType.REMOVE_HEADER.toString());
      builder.configure(new Context(contextMap));
      return builder.build();
    }
  }
}
