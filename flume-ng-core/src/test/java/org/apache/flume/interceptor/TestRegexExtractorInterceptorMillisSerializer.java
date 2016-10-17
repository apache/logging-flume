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

import junit.framework.Assert;
import org.apache.flume.Context;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

public class TestRegexExtractorInterceptorMillisSerializer {

  @Test
  public void shouldRequirePatternInConfiguration() {
    try {
      RegexExtractorInterceptorMillisSerializer fixture =
          new RegexExtractorInterceptorMillisSerializer();
      fixture.configure(new Context());
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected...
    }
  }

  @Test
  public void shouldRequireValidPatternInConfiguration() {
    try {
      RegexExtractorInterceptorMillisSerializer fixture =
          new RegexExtractorInterceptorMillisSerializer();
      Context context = new Context();
      context.put("pattern", "ABCDEFG");
      fixture.configure(context);
      Assert.fail();
    } catch (IllegalArgumentException ex) {
      // Expected...
    }
  }

  @Test
  public void shouldReturnMillisFromPattern() {
    RegexExtractorInterceptorMillisSerializer fixture =
        new RegexExtractorInterceptorMillisSerializer();
    Context context = new Context();
    String pattern = "yyyy-MM-dd HH:mm:ss";
    context.put("pattern", pattern);
    fixture.configure(context);

    DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern);
    long time = (System.currentTimeMillis() / 1000L) * 1000L;
    Assert.assertEquals(String.valueOf(time),
        fixture.serialize(formatter.print(time)));
  }
}
