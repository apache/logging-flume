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
package org.apache.flume.channel.redis;

import org.apache.flume.channels.redis.tools.SingleServerRedisOperator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer;

import java.io.IOException;

public class TestSingleRedis {
  private RedisServer redisServer;
  private SingleServerRedisOperator singleServerRedisOperator;
  
  @Before
  public void setUp() throws IOException {
    RedisExecProvider customProvider = RedisExecProvider.defaultProvider();
    redisServer = RedisServer.builder().redisExecProvider(customProvider)
        .setting("maxheap 128M").build();
    redisServer.start();
    singleServerRedisOperator = new SingleServerRedisOperator("localhost", 6379, "", 1000, null);
  }

  @Test
  public void testSimpleListOperator() {
    Assert.assertEquals(1,singleServerRedisOperator.lpush("test", "a"));
    Assert.assertEquals(1, Integer.parseInt(singleServerRedisOperator.llen("test").toString()));
    Assert.assertEquals("a", singleServerRedisOperator.rpop("test"));
    Assert.assertEquals(0, Integer.parseInt(singleServerRedisOperator.llen("test").toString()));    
  }
  
  @Test
  public void testComplexListOperator() {
    String[] multiPush = {"a","b","c","d","e"};
    Assert.assertEquals(multiPush.length,singleServerRedisOperator.lpush("test", multiPush));
    Assert.assertEquals(multiPush.length,
                        Integer.parseInt(singleServerRedisOperator.llen("test").toString()));
    Assert.assertEquals(multiPush[0], singleServerRedisOperator.rpop("test"));
    Assert.assertEquals(multiPush.length - 1,
                        Integer.parseInt(singleServerRedisOperator.llen("test").toString()));
    
  }

  @After
  public void tearDown() {
    redisServer.stop();
  }

}
