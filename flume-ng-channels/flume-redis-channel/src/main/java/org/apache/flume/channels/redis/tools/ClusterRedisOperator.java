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

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.HostAndPort;

public class ClusterRedisOperator implements RedisOperator {
  private static final Logger log = LoggerFactory.getLogger(RedisInit.class);
  private JedisCluster jedis;

  public ClusterRedisOperator(String servers, String passwd, int timeout,
                              int maxAttemp, JedisPoolConfig jedisPoolConfig) {
    Set<HostAndPort> nodeSet = new HashSet<>();
    String[] serverStrings = servers.split(",");
    for (String ipPort : serverStrings) {
      String[] ipPortPair = ipPort.split(":");
      nodeSet.add(new redis.clients.jedis.HostAndPort(ipPortPair[0].trim(),
          Integer.valueOf(ipPortPair[1].trim())));
    }
    if (StringUtils.isBlank(passwd)) {
      jedis = new JedisCluster(nodeSet, timeout, timeout, maxAttemp,
          jedisPoolConfig);
    } else {
      jedis = new JedisCluster(nodeSet, timeout, timeout, maxAttemp, passwd,
          jedisPoolConfig);
    }
  }

  public long lpush(String key, String... strings) {
    long count = 0;
    count = jedis.lpush(key, strings);
    return count;
  }

  public String rpoplpush(String srckey, String dstkey) {
    String msg = "";
    msg = jedis.rpoplpush(srckey, dstkey);
    return msg;
  }

  public String rpop(String key) {
    String msg = "";
    msg = jedis.rpop(key);
    return msg;
  }

  public Long llen(String key) {
    long result;
    result = jedis.llen(key);
    return result;
  }
}
