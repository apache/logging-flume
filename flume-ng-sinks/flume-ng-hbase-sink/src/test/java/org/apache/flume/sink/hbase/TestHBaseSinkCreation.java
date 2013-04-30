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
package org.apache.flume.sink.hbase;

import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.apache.flume.sink.DefaultSinkFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestHBaseSinkCreation {

  private SinkFactory sinkFactory;

  @Before
  public void setUp() {
    sinkFactory = new DefaultSinkFactory();
  }

  private void verifySinkCreation(String name, String type,
      Class<?> typeClass) throws FlumeException {
    Sink sink = sinkFactory.create(name, type);
    Assert.assertNotNull(sink);
    Assert.assertTrue(typeClass.isInstance(sink));
  }

  @Test
  public void testSinkCreation() {
    verifySinkCreation("hbase-sink", "hbase", HBaseSink.class);
    verifySinkCreation("asynchbase-sink", "asynchbase", AsyncHBaseSink.class);
  }
}
