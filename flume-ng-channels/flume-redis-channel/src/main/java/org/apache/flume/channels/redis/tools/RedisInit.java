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

import java.util.HashMap;
import java.util.Properties;

import javax.naming.ConfigurationException;

import redis.clients.jedis.JedisPoolConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.channels.redis.exception.CannotGetRedisInstanceException;
import org.apache.flume.channels.redis.exception.UnsupportRedisTypeException;

import static org.apache.flume.channels.redis.RedisChannelConfiguration.*;

public class RedisInit {
  private static final Logger LOGGER = LoggerFactory.getLogger(RedisInit.class);
  private static HashMap<String, RedisOperator> instances = new HashMap<String, RedisOperator>();

  public static synchronized RedisOperator getInstance(Properties redisConf,
                                                       JedisPoolConfig jedisPoolConfig)
      throws UnsupportRedisTypeException, CannotGetRedisInstanceException {
    String type = redisConf.getProperty(SERVER_TYPE);
    String hosts = redisConf.getProperty(REDIS_SERVER);
    String passwd = redisConf.getProperty(REDIS_PASSWORD);
    int timeout = Integer.parseInt(redisConf.getProperty(REDIS_CONNTIMOUT));
    String instanceKey = type + "|" + hosts;
    RedisOperator ro = null;
    if (instances.containsKey(instanceKey)) {
      ro = instances.get(instanceKey);
    } else {
      if (null == jedisPoolConfig) {
        jedisPoolConfig = new JedisPoolConfig();
      }
      if (type.equals(DEFAULT_SEVER_TYPE)) {
        String[] hostAndPort = hosts.split(":");
        ro = new SingleServerRedisOperator(hostAndPort[0].trim(),
            Integer.valueOf(hostAndPort[1].trim()), passwd, timeout,
            jedisPoolConfig);
      } else if (type.equals(CLUSTER_SERVER_TYPE)) {
        int maxAttemp = Integer
            .parseInt(redisConf.getProperty(CLUSTER_MAX_ATTEMP));
        ro = new ClusterRedisOperator(hosts, passwd, timeout, maxAttemp,
            jedisPoolConfig);
      } else if (type.equals(SENTINEL_SERVER_TYPE)) {
        String masterName = redisConf.getProperty(SENTINEL_MASTER_NAME);
        ro = new SentinelServerRedisOperator(masterName, hosts, passwd, timeout,
            jedisPoolConfig);
      } else {
        throw new UnsupportRedisTypeException("Unsupport redis type " + type);
      }
      instances.put(instanceKey, ro);
    }
    if (null == ro) {
      throw new CannotGetRedisInstanceException(
          "Can not get redis instance with type: " + type + " and servers: "
              + hosts);
    }
    return ro;
  }
}
