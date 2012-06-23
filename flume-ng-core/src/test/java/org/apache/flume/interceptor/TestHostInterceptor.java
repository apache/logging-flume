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

import java.net.InetAddress;

import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.HostInterceptor.Constants;
import org.junit.Assert;
import org.junit.Test;

public class TestHostInterceptor {

  /**
   * Ensure that the "host" header gets set (to something)
   */
  @Test
  public void testBasic() throws Exception {
    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
            InterceptorType.HOST.toString());
    Interceptor interceptor = builder.build();

    Event eventBeforeIntercept = EventBuilder.withBody("test event",
            Charsets.UTF_8);
    Assert.assertNull(eventBeforeIntercept.getHeaders().get(Constants.HOST));

    Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
    String actualHost = eventAfterIntercept.getHeaders().get(Constants.HOST);

    Assert.assertNotNull(actualHost);
  }

  @Test
  public void testCustomHeader() throws Exception {
    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
            InterceptorType.HOST.toString());
    Context ctx = new Context();
    ctx.put("preserveExisting", "false");
    ctx.put("hostHeader", "hostname");
    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    Event eventBeforeIntercept = EventBuilder.withBody("test event",
            Charsets.UTF_8);
    Assert.assertNull(eventBeforeIntercept.getHeaders().get("hostname"));

    Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
    String actualHost = eventAfterIntercept.getHeaders().get("hostname");

    Assert.assertNotNull(actualHost);
    Assert.assertEquals(InetAddress.getLocalHost().getHostAddress(),
        actualHost);
  }

  /**
   * Ensure host is NOT overwritten when preserveExisting=true.
   */
  @Test
  public void testPreserve() throws Exception {
    Context ctx = new Context();
    ctx.put("preserveExisting", "true");

    Interceptor.Builder builder = InterceptorBuilderFactory.newInstance(
            InterceptorType.HOST.toString());
    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    final String ORIGINAL_HOST = "originalhost";
    Event eventBeforeIntercept = EventBuilder.withBody("test event",
            Charsets.UTF_8);
    eventBeforeIntercept.getHeaders().put(Constants.HOST, ORIGINAL_HOST);
    Assert.assertEquals(ORIGINAL_HOST,
            eventBeforeIntercept.getHeaders().get(Constants.HOST));

    String expectedHost = ORIGINAL_HOST;
    Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
    String actualHost = eventAfterIntercept.getHeaders().get(Constants.HOST);

    Assert.assertNotNull(actualHost);
    Assert.assertEquals(expectedHost, actualHost);
  }

  /**
   * Ensure host IS overwritten when preserveExisting=false.
   */
  @Test
  public void testClobber() throws Exception {
    Context ctx = new Context();
    ctx.put("preserveExisting", "false"); // default behavior

    Interceptor.Builder builder = InterceptorBuilderFactory
            .newInstance(InterceptorType.HOST.toString());
    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    final String ORIGINAL_HOST = "originalhost";
    Event eventBeforeIntercept = EventBuilder.withBody("test event",
            Charsets.UTF_8);
    eventBeforeIntercept.getHeaders().put(Constants.HOST, ORIGINAL_HOST);
    Assert.assertEquals(ORIGINAL_HOST, eventBeforeIntercept.getHeaders()
            .get(Constants.HOST));

    String expectedHost = InetAddress.getLocalHost().getHostAddress();
    Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
    String actualHost = eventAfterIntercept.getHeaders().get(Constants.HOST);

    Assert.assertNotNull(actualHost);
    Assert.assertEquals(expectedHost, actualHost);
  }

  /**
   * Ensure host IP is used by default instead of host name.
   */
  @Test
  public void testUseIP() throws Exception {
    Context ctx = new Context();
    ctx.put("useIP", "true"); // default behavior

    Interceptor.Builder builder = InterceptorBuilderFactory
            .newInstance(InterceptorType.HOST.toString());
    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    final String ORIGINAL_HOST = "originalhost";
    Event eventBeforeIntercept = EventBuilder.withBody("test event",
            Charsets.UTF_8);
    eventBeforeIntercept.getHeaders().put(Constants.HOST, ORIGINAL_HOST);
    Assert.assertEquals(ORIGINAL_HOST, eventBeforeIntercept.getHeaders()
            .get(Constants.HOST));

    String expectedHost = InetAddress.getLocalHost().getHostAddress();
    Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
    String actualHost = eventAfterIntercept.getHeaders().get(Constants.HOST);

    Assert.assertNotNull(actualHost);
    Assert.assertEquals(expectedHost, actualHost);
  }

  /**
   * Ensure host name can be used instead of host IP.
   */
  @Test
  public void testUseHostname() throws Exception {
    Context ctx = new Context();
    ctx.put("useIP", "false");

    Interceptor.Builder builder = InterceptorBuilderFactory
            .newInstance(InterceptorType.HOST.toString());
    builder.configure(ctx);
    Interceptor interceptor = builder.build();

    final String ORIGINAL_HOST = "originalhost";
    Event eventBeforeIntercept = EventBuilder.withBody("test event",
            Charsets.UTF_8);
    eventBeforeIntercept.getHeaders().put(Constants.HOST, ORIGINAL_HOST);
    Assert.assertEquals(ORIGINAL_HOST, eventBeforeIntercept.getHeaders()
            .get(Constants.HOST));

    String expectedHost = InetAddress.getLocalHost().getCanonicalHostName();
    Event eventAfterIntercept = interceptor.intercept(eventBeforeIntercept);
    String actualHost = eventAfterIntercept.getHeaders().get(Constants.HOST);

    Assert.assertNotNull(actualHost);
    Assert.assertEquals(expectedHost, actualHost);
  }

}
