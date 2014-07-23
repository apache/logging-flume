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
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

public class TestSearchAndReplaceInterceptor {

  private static final Logger logger =
      LoggerFactory.getLogger(TestSearchAndReplaceInterceptor.class);

  private void testSearchReplace(Context context, String input, String output)
      throws Exception {
   Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
        InterceptorType.SEARCH_REPLACE.toString());
    builder.configure(context);
    Interceptor interceptor = builder.build();

    Event event = EventBuilder.withBody(input, Charsets.UTF_8);
    event = interceptor.intercept(event);
    String val = new String(event.getBody(), Charsets.UTF_8);
    assertEquals(output, val);
    logger.info(val);
  }

  @Test
  public void testRemovePrefix() throws Exception {
    Context context = new Context();
    context.put("searchPattern", "^prefix");
    context.put("replaceString", "");
    testSearchReplace(context, "prefix non-prefix suffix", " non-prefix suffix");
  }

  @Test
  public void testSyslogStripPriority() throws Exception {
    final String input = "<13>Feb  5 17:32:18 10.0.0.99 Use the BFG!";
    final String output = "Feb  5 17:32:18 10.0.0.99 Use the BFG!";
    Context context = new Context();
    context.put("searchPattern", "^<[0-9]+>");
    context.put("replaceString", "");
    testSearchReplace(context, input, output);
  }

  @Test
  public void testCapturedGroups() throws Exception {
    final String input = "The quick brown fox jumped over the lazy dog.";
    final String output = "The hungry dog ate the careless fox.";
    Context context = new Context();
    context.put("searchPattern", "The quick brown ([a-z]+) jumped over the lazy ([a-z]+).");
    context.put("replaceString", "The hungry $2 ate the careless $1.");
    testSearchReplace(context, input, output);
  }

  @Test
  public void testRepeatedRemoval() throws Exception {
    final String input = "Email addresses: test@test.com and foo@test.com";
    final String output = "Email addresses: REDACTED and REDACTED";
    Context context = new Context();
    context.put("searchPattern", "[A-Za-z0-9_.]+@[A-Za-z0-9_-]+\\.com");
    context.put("replaceString", "REDACTED");
    testSearchReplace(context, input, output);
  }
}