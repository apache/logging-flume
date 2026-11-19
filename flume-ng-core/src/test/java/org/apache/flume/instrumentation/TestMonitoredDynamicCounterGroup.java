/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.instrumentation;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class TestMonitoredDynamicCounterGroup {

  private MBeanServer mbServer;
  private final String CATEGORY_A = "categoryA";
  private final String CATEGORY_B = "categoryB";
  private final String OBJ_NAME = "org.apache.flume.interceptor:type=";

  public class ExampleInterceptorCounter extends MonitoredDynamicCounterGroup {

    protected ExampleInterceptorCounter(String name) {
      super(MonitoredCounterGroup.Type.INTERCEPTOR, name);
    }
  }

  @Before
  public void setUp() {
    mbServer = ManagementFactory.getPlatformMBeanServer();
  }

  @Test
  public void testInterceptorCounter()
      throws Exception {
    String name = String.valueOf(System.nanoTime());
    ExampleInterceptorCounter counter =
        new ExampleInterceptorCounter(name);
    counter.start();
    counter.increment(CATEGORY_A);
    counter.increment(CATEGORY_A);
    counter.increment(CATEGORY_B);

    ObjectName ob = new ObjectName(OBJ_NAME + name);

    // Verify
    assertCounter(ob, CATEGORY_A, 2L);
    assertCounter(ob, CATEGORY_B, 1L);
  }

  private void assertCounter(ObjectName ob, String name, Long count)
      throws Exception {
    Assert.assertEquals(getLongAttribute(ob, name), count);
  }

  private Long getLongAttribute(ObjectName on, String attr) throws Exception {
    Object result = getAttribute(on, attr);
    return ((Long) result).longValue();
  }

  private Object getAttribute(ObjectName objName, String attrName)
      throws Exception {
    return mbServer.getAttribute(objName, attrName);
  }

}
