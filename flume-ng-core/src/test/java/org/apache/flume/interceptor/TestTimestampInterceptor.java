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
import org.apache.flume.interceptor.TimestampInterceptor.Constants;
import org.junit.Assert;
import org.junit.Test;

public class TestTimestampInterceptor {

  /**
   * Ensure that the "timestamp" header gets set (to something)
   */
  @Test
  public void testBasic() throws ClassNotFoundException, InstantiationException,
      IllegalAccessException {

    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
        InterceptorType.TIMESTAMP.toString());
    Interceptor interceptor = builder.build();

    Event event = EventBuilder.withBody("test event", Charsets.UTF_8);
    Assert.assertNull(event.getHeaders().get(Constants.DEFAULT_HEADER_NAME));

    Long now = System.currentTimeMillis();
    event = interceptor.intercept(event);
    String timestampStr = event.getHeaders().get(Constants.DEFAULT_HEADER_NAME);
    Assert.assertNotNull(timestampStr);
    Assert.assertTrue(Long.parseLong(timestampStr) >= now);
  }

  /**
   * Ensure timestamp is NOT overwritten when preserveExistingTimestamp == true
   */
  @Test
  public void testPreserve() throws ClassNotFoundException,
      InstantiationException, IllegalAccessException {

    Context ctx = new Context();
    ctx.put("preserveExisting", "true");

    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
        InterceptorType.TIMESTAMP.toString());
    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    long originalTs = 1L;
    Event event = EventBuilder.withBody("test event", Charsets.UTF_8);
    event.getHeaders().put(Constants.DEFAULT_HEADER_NAME, Long.toString(originalTs));
    Assert.assertEquals(Long.toString(originalTs),
        event.getHeaders().get(Constants.DEFAULT_HEADER_NAME));

    event = interceptor.intercept(event);
    String timestampStr = event.getHeaders().get(Constants.DEFAULT_HEADER_NAME);
    Assert.assertNotNull(timestampStr);
    Assert.assertTrue(Long.parseLong(timestampStr) == originalTs);
  }

  /**
   * Ensure timestamp IS overwritten when preserveExistingTimestamp == false
   */
  @Test
  public void testClobber() throws ClassNotFoundException,
      InstantiationException, IllegalAccessException {

    Context ctx = new Context();
    ctx.put("preserveExisting", "false"); // DEFAULT BEHAVIOR

    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
        InterceptorType.TIMESTAMP.toString());
    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    long originalTs = 1L;
    Event event = EventBuilder.withBody("test event", Charsets.UTF_8);
    event.getHeaders().put(Constants.DEFAULT_HEADER_NAME, Long.toString(originalTs));
    Assert.assertEquals(Long.toString(originalTs),
        event.getHeaders().get(Constants.DEFAULT_HEADER_NAME));

    Long now = System.currentTimeMillis();
    event = interceptor.intercept(event);
    String timestampStr = event.getHeaders().get(Constants.DEFAULT_HEADER_NAME);
    Assert.assertNotNull(timestampStr);
    Assert.assertTrue(Long.parseLong(timestampStr) >= now);
  }

  @Test
  public void testCustomHeader() throws Exception {
    Context ctx = new Context();
    ctx.put(TimestampInterceptor.Constants.CONFIG_HEADER_NAME, "timestampHeader");
    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
            InterceptorType.TIMESTAMP.toString());
    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    long originalTs = 1L;
    Event event = EventBuilder.withBody("test event", Charsets.UTF_8);
    event.getHeaders().put(Constants.DEFAULT_HEADER_NAME, Long.toString(originalTs));

    Long now = System.currentTimeMillis();
    event = interceptor.intercept(event);
    Assert.assertEquals(Long.toString(originalTs),
            event.getHeaders().get(Constants.DEFAULT_HEADER_NAME));
    String timestampStr = event.getHeaders().get("timestampHeader");
    Assert.assertNotNull(timestampStr);
    Assert.assertTrue(Long.parseLong(timestampStr) >= now);
  }
}
