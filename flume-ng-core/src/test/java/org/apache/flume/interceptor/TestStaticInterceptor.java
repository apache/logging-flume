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
import org.apache.flume.interceptor.StaticInterceptor.Constants;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TestStaticInterceptor {
  @Test
  public void testDefaultKeyValue() throws ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
        InterceptorType.STATIC.toString());
    builder.configure(new Context());
    Interceptor interceptor = builder.build();

    Event event = EventBuilder.withBody("test", Charsets.UTF_8);
    Assert.assertNull(event.getHeaders().get(Constants.KEY));

    event = interceptor.intercept(event);
    String val = event.getHeaders().get(Constants.KEY);

    Assert.assertNotNull(val);
    Assert.assertEquals(Constants.VALUE, val);
  }

  @Test
  public void testCustomKeyValue() throws ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
        InterceptorType.STATIC.toString());
    Context ctx = new Context();
    ctx.put(Constants.KEY, "myKey");
    ctx.put(Constants.VALUE, "myVal");

    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    Event event = EventBuilder.withBody("test", Charsets.UTF_8);
    Assert.assertNull(event.getHeaders().get("myKey"));

    event = interceptor.intercept(event);
    String val = event.getHeaders().get("myKey");

    Assert.assertNotNull(val);
    Assert.assertEquals("myVal", val);
  }

  @Test
  public void testReplace() throws ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
        InterceptorType.STATIC.toString());
    Context ctx = new Context();
    ctx.put(Constants.PRESERVE, "false");
    ctx.put(Constants.VALUE, "replacement value");

    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    Event event = EventBuilder.withBody("test", Charsets.UTF_8);
    event.getHeaders().put(Constants.KEY, "incumbent value");

    Assert.assertNotNull(event.getHeaders().get(Constants.KEY));

    event = interceptor.intercept(event);
    String val = event.getHeaders().get(Constants.KEY);

    Assert.assertNotNull(val);
    Assert.assertEquals("replacement value", val);
  }

  @Test
  public void testPreserve() throws ClassNotFoundException,
      InstantiationException, IllegalAccessException {
    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
        InterceptorType.STATIC.toString());
    Context ctx = new Context();
    ctx.put(Constants.PRESERVE, "true");
    ctx.put(Constants.VALUE, "replacement value");

    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    Event event = EventBuilder.withBody("test", Charsets.UTF_8);
    event.getHeaders().put(Constants.KEY, "incumbent value");

    Assert.assertNotNull(event.getHeaders().get(Constants.KEY));

    event = interceptor.intercept(event);
    String val = event.getHeaders().get(Constants.KEY);

    Assert.assertNotNull(val);
    Assert.assertEquals("incumbent value", val);
  }
}