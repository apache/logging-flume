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
package org.apache.flume.agent.embedded;

import java.util.Map;

import junit.framework.Assert;

import org.apache.flume.FlumeException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;

public class TestEmbeddedAgentConfiguration {
  private Map<String, String> properties;

  @Before
  public void setUp() throws Exception {
    properties = Maps.newHashMap();
    properties.put("source.type", EmbeddedAgentConfiguration.
        SOURCE_TYPE_EMBEDDED);
    properties.put("channel.type", "memory");
    properties.put("channel.capacity", "200");
    properties.put("sinks", "sink1 sink2");
    properties.put("sink1.type", "avro");
    properties.put("sink2.type", "avro");
    properties.put("sink1.hostname", "sink1.host");
    properties.put("sink1.port", "2");
    properties.put("sink2.hostname", "sink2.host");
    properties.put("sink2.port", "2");
    properties.put("processor.type", "load_balance");
  }


  @Test
  public void testFullSourceType() throws Exception {
    doTestExcepted(EmbeddedAgentConfiguration.
        configure("test1", properties));
  }

  @Test
  public void testMissingSourceType() throws Exception {
    Assert.assertNotNull(properties.remove("source.type"));
    doTestExcepted(EmbeddedAgentConfiguration.
        configure("test1", properties));
  }

  @Test
  public void testShortSourceType() throws Exception {
    properties.put("source.type", "EMBEDDED");
    doTestExcepted(EmbeddedAgentConfiguration.
        configure("test1", properties));
  }


  public void doTestExcepted(Map<String, String> actual) throws Exception {
    Map<String, String> expected = Maps.newHashMap();
    expected.put("test1.channels", "channel-test1");
    expected.put("test1.channels.channel-test1.capacity", "200");
    expected.put("test1.channels.channel-test1.type", "memory");
    expected.put("test1.sinkgroups", "sink-group-test1");
    expected.put("test1.sinkgroups.sink-group-test1.processor.type", "load_balance");
    expected.put("test1.sinkgroups.sink-group-test1.sinks", "sink1 sink2");
    expected.put("test1.sinks", "sink1 sink2");
    expected.put("test1.sinks.sink1.channel", "channel-test1");
    expected.put("test1.sinks.sink1.hostname", "sink1.host");
    expected.put("test1.sinks.sink1.port", "2");
    expected.put("test1.sinks.sink1.type", "avro");
    expected.put("test1.sinks.sink2.channel", "channel-test1");
    expected.put("test1.sinks.sink2.hostname", "sink2.host");
    expected.put("test1.sinks.sink2.port", "2");
    expected.put("test1.sinks.sink2.type", "avro");
    expected.put("test1.sources", "source-test1");
    expected.put("test1.sources.source-test1.channels", "channel-test1");
    expected.put("test1.sources.source-test1.type", EmbeddedAgentConfiguration.
        SOURCE_TYPE_EMBEDDED);
    Assert.assertEquals(expected, actual);
  }

  @Test(expected = FlumeException.class)
  public void testBadSource() throws Exception {
    properties.put("source.type", "exec");
    EmbeddedAgentConfiguration.configure("test1", properties);
  }
  @Test(expected = FlumeException.class)
  public void testBadChannel() throws Exception {
    properties.put("channel.type", "jdbc");
    EmbeddedAgentConfiguration.configure("test1", properties);
  }
  @Test(expected = FlumeException.class)
  public void testBadSink() throws Exception {
    properties.put("sink1.type", "hbase");
    EmbeddedAgentConfiguration.configure("test1", properties);
  }
  @Test(expected = FlumeException.class)
  public void testBadSinkProcessor() throws Exception {
    properties.put("processor.type", "bad");
    EmbeddedAgentConfiguration.configure("test1", properties);
  }

  @Test(expected = FlumeException.class)
  public void testNoChannel() throws Exception {
    properties.remove("channel.type");
    EmbeddedAgentConfiguration.configure("test1", properties);
  }
  @Test(expected = FlumeException.class)
  public void testNoSink() throws Exception {
    properties.remove("sink2.type");
    EmbeddedAgentConfiguration.configure("test1", properties);
  }
  @Test(expected = FlumeException.class)
  public void testNoSinkProcessor() throws Exception {
    properties.remove("processor.type");
    EmbeddedAgentConfiguration.configure("test1", properties);
  }
  @Test(expected = FlumeException.class)
  public void testBadKey() throws Exception {
    properties.put("bad.key.name", "bad");
    EmbeddedAgentConfiguration.configure("test1", properties);
  }
  @Test(expected = FlumeException.class)
  public void testSinkNamedLikeSource() throws Exception {
    properties.put("sinks", "source");
    EmbeddedAgentConfiguration.configure("test1", properties);
  }
  @Test(expected = FlumeException.class)
  public void testSinkNamedLikeChannel() throws Exception {
    properties.put("sinks", "channel");
    EmbeddedAgentConfiguration.configure("test1", properties);
  }
  @Test(expected = FlumeException.class)
  public void testSinkNamedLikeProcessor() throws Exception {
    properties.put("sinks", "processor");
    EmbeddedAgentConfiguration.configure("test1", properties);
  }
}