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

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

public class SentinelRedisController {
  private JedisSentinelPool jsp;

  public SentinelRedisController(String masterName, String servers,
      String passwd, int timeout, JedisPoolConfig jedisPoolConfig) {
    Set<String> sentinels = new HashSet<String>();
    String[] arr = servers.split(",");
    if (arr != null && arr.length > 0) {
      for (String item : arr) {
        if (item != null && !item.equals("")) {
          sentinels.add(item);
        }
      }
    }
    if (StringUtils.isBlank(passwd)) {
      jsp = new JedisSentinelPool(masterName, sentinels, jedisPoolConfig,
          timeout);

    } else {
      jsp = new JedisSentinelPool(masterName, sentinels, jedisPoolConfig,
          timeout, passwd);
    }
  }

  public void destory() {
    jsp.destroy();
  }

  public Jedis getController() {
    return jsp.getResource();
  }

  public void returnController(Jedis _jedis) {
    _jedis.close();
  }
}
