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
package org.apache.flume.channels.redis;

public class RedisChannelConfiguration {

  public static final String RREDIS_PREFIX = "redis.";
  public static final String REDIS_SENTINEL_SERVER = "sentinel.servers";
  public static final String REDIS_SERVER= "server";
  public static final String REDIS_PORT = "port";
  public static final String REDIS_MASTER_NAME = "master.name";
  public static final String REDIS_PASSWORD = "password";
  public static final String QUEUE_KEY = "key";
  public static final String BATCH_NUMBER = "batch_number";
  public static final String DEFALUT_BATCH_NUMBER = "10";
  public static final String DEFALUT_REIDS_PORT = "6379";
  
  public static final String PARSE_AS_FLUME_EVENT = "parseAsFlumeEvent";
  public static final boolean DEFAULT_PARSE_AS_FLUME_EVENT = true;
}
