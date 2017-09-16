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
package org.apache.flume.channels.redis.tools;

import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class RedisController {
	private JedisPool jsp;
	private static RedisController rc = null;
	private int redisConnTimeout;
	private int maxTotal;
	private int maxIdle;
	private int minIdle;
	private int maxWaitMillis;

	private JedisPoolConfig createJedisConfig() {
		redisConnTimeout = 60000;
		maxTotal = 500;
		maxIdle = 300;
		minIdle = 10;
		maxWaitMillis = 60000;
		JedisPoolConfig jedisConfig = new JedisPoolConfig();
		jedisConfig.setMaxTotal(maxTotal);
		jedisConfig.setMaxIdle(maxIdle);
		jedisConfig.setMinIdle(minIdle);
		jedisConfig.setMaxWaitMillis(maxWaitMillis);
		jedisConfig.setTestOnBorrow(true);
		jedisConfig.setTestOnReturn(true);
		jedisConfig.setTestWhileIdle(true);
		jedisConfig.setTimeBetweenEvictionRunsMillis(30000);
		jedisConfig.setNumTestsPerEvictionRun(10);
		jedisConfig.setMinEvictableIdleTimeMillis(60000);
		return jedisConfig;
	}

	public static RedisController getRedisController(String host, int port,
			String passwd) {
		if (rc == null) {
			rc = new RedisController(host, port, passwd);
		}
		return rc;
	}

	private RedisController(String host, int port, String passwd) {
		if (!StringUtils.isBlank(passwd)) {
			jsp = new JedisPool(createJedisConfig(), host, port,
					redisConnTimeout, passwd);

		} else {
			jsp = new JedisPool(createJedisConfig(), host, port,
					redisConnTimeout);
		}
	}

	public void destory() {
		jsp.destroy();
	}

	public Jedis getController() {
		return jsp.getResource();
	}

	public void returnController(Jedis _jedis) {
		// jsp.returnBrokenResource(_jedis);
		_jedis.close();
	}
}
