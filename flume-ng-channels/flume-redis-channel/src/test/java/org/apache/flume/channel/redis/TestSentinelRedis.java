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

import org.apache.flume.channels.redis.tools.SentinelServerRedisOperator;
import org.apache.flume.channels.redis.tools.SingleServerRedisOperator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.JedisPoolConfig;
import redis.embedded.RedisCluster;
import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer;
import redis.embedded.util.JedisUtil;

import java.io.IOException;
import java.util.Set;

public class TestSentinelRedis {
	private RedisCluster cluster;
	private Set<String> jedisSentinelHosts;
	private SentinelServerRedisOperator sentinelServerRedisOpertor;

	@Before
	public void setUp() throws IOException {
		cluster = RedisCluster.builder().ephemeral().sentinelCount(3).quorumSize(2).replicationGroup("master1", 1)
				.replicationGroup("master2", 1).replicationGroup("master3", 1).build();
		cluster.start();
		jedisSentinelHosts = JedisUtil.sentinelHosts(cluster);
		StringBuilder sb = new StringBuilder();
		for (String host : jedisSentinelHosts) {
			sb.append(host + ",");
		}
		sentinelServerRedisOpertor = new SentinelServerRedisOperator("master1",
				sb.toString(), "", 5000, new JedisPoolConfig());
	}

	@Test
	public void testSimpleListOperator() {
		Assert.assertEquals(1, sentinelServerRedisOpertor.lpush("test", "a"));
		Assert.assertEquals(1, Integer.parseInt(sentinelServerRedisOpertor.llen("test").toString()));
		Assert.assertEquals("a", sentinelServerRedisOpertor.rpop("test"));
		Assert.assertEquals(0, Integer.parseInt(sentinelServerRedisOpertor.llen("test").toString()));
	}

	@Test
	public void testComplexListOperator() {
		String[] multiPush = { "a", "b", "c", "d", "e" };
		Assert.assertEquals(multiPush.length, sentinelServerRedisOpertor.lpush("test", multiPush));
		Assert.assertEquals(multiPush.length, Integer.parseInt(sentinelServerRedisOpertor.llen("test").toString()));
		Assert.assertEquals(multiPush[0], sentinelServerRedisOpertor.rpop("test"));
		Assert.assertEquals(multiPush.length - 1, Integer.parseInt(sentinelServerRedisOpertor.llen("test").toString()));

	}

	@After
	public void tearDown() {
		/* cluster.stop(); */
	}

}
