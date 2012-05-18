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
package org.apache.flume.sink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.apache.flume.SinkProcessor;
import org.junit.Assert;
import org.junit.Test;

public class SinkProcessorFactoryTest {

  @Test
  public void test() {
    Context context = new Context();
    context.put("type", FailoverSinkProcessor.class.getName());
    context.put("priority.sink1", "1");
    context.put("priority.sink2", "2");
    SinkFactory sf = new DefaultSinkFactory();
    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(sf.create("sink1", "avro"));
    sinks.add(sf.create("sink2", "avro"));
    SinkProcessor sp = SinkProcessorFactory.getProcessor(context, sinks);
    context.put("type", "failover");
    SinkProcessor sp2 = SinkProcessorFactory.getProcessor(context, sinks);
    Assert.assertEquals(sp.getClass(), sp2.getClass());
  }

  @Test
  public void testInstantiatingLoadBalancingSinkProcessor() {
    Context context = new Context();
    context.put("type", LoadBalancingSinkProcessor.class.getName());
    context.put("selector", "random");
    SinkFactory sf = new DefaultSinkFactory();
    List<Sink> sinks = new ArrayList<Sink>();
    sinks.add(sf.create("sink1", "avro"));
    sinks.add(sf.create("sink2", "avro"));
    SinkProcessor sp = SinkProcessorFactory.getProcessor(context, sinks);
    context.put("type", "load_balance");
    SinkProcessor sp2 = SinkProcessorFactory.getProcessor(context, sinks);
    Assert.assertEquals(sp.getClass(), sp2.getClass());
  }

}
