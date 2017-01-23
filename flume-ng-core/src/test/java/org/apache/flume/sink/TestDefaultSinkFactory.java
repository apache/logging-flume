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

import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDefaultSinkFactory {

  private SinkFactory sinkFactory;

  @Before
  public void setUp() {
    sinkFactory = new DefaultSinkFactory();
  }

  @Test
  public void testDuplicateCreate() {

    Sink avroSink1 = sinkFactory.create("avroSink1", "avro");
    Sink avroSink2 = sinkFactory.create("avroSink2", "avro");

    Assert.assertNotNull(avroSink1);
    Assert.assertNotNull(avroSink2);
    Assert.assertNotSame(avroSink1, avroSink2);
    Assert.assertTrue(avroSink1 instanceof AvroSink);
    Assert.assertTrue(avroSink2 instanceof AvroSink);

    Sink s1 = sinkFactory.create("avroSink1", "avro");
    Sink s2 = sinkFactory.create("avroSink2", "avro");

    Assert.assertNotSame(avroSink1, s1);
    Assert.assertNotSame(avroSink2, s2);
  }

  private void verifySinkCreation(String name, String type, Class<?> typeClass)
      throws Exception {
    Sink sink = sinkFactory.create(name, type);
    Assert.assertNotNull(sink);
    Assert.assertTrue(typeClass.isInstance(sink));
  }

  @Test
  public void testSinkCreation() throws Exception {
    verifySinkCreation("null-sink", "null", NullSink.class);
    verifySinkCreation("logger-sink", "logger", LoggerSink.class);
    verifySinkCreation("file-roll-sink", "file_roll", RollingFileSink.class);
    verifySinkCreation("avro-sink", "avro", AvroSink.class);
  }

}
