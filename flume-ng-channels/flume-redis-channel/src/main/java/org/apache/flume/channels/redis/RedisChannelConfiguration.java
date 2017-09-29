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
  public static final String SERVER_TYPE = "server.type";
  public static final String REDIS_CLUSTER_SERVER = "cluster.servers";
  public static final String REDIS_SERVER = "single.server";
  public static final String REDIS_SENTINEL_SERVER = "sentinel.servers";
  public static final String SENTINEL_MASTER_NAME = "sentinel.master.name";
  public static final String REDIS_PASSWORD = "password";
  public static final String QUEUE_KEY = "key";
  public static final String BATCH_NUMBER = "batch_number";
  public static final String DEFAULT_BATCH_NUMBER = "10";
  public static final String DEFAULT_REIDS_PORT = "6379";
  public static final String DEFAULT_SEVER_TYPE = "single";
  public static final String CLUSTER_SERVER_TYPE = "cluster";
  public static final String SENTINEL_SERVER_TYPE = "sentinel";
  
  public static final String RREDIS_PREFIX = "redis.";  
  public static final String REDIS_CONNTIMOUT = "timeout";
  public static final String MAX_TO_TOTAL = "max.total";
  public static final String MAX_IDLE = "max.idle";
  public static final String MIN_IDLE = "min.idle";
  public static final String MAX_WAIT_MILLIS = "max.wait.millis";
  public static final String TEST_ON_BORROW = "test.on.borrow";
  public static final String TEST_ON_RETURN = "test.on.return";
  public static final String TEST_WHILE_IDLE = "test.while.idle";
  public static final String TIME_BETWEEN_EVICTION_RUNMILLIS = "time.between.eviction.runs.millis";
  public static final String NUM_TESTS_PER_EVICTION_RUN = "num.tests.per.eviction.run";
  public static final String MIN_EVICTABLE_IDLE_TIME_MILLS = "min.evictable.idle.time.millis";
  public static final String CLUSTER_MAX_ATTEMP = "cluster.max.attemp";
  public static final String DEFAULT_REDIS_CONNTIMEOUT = "5000";
  public static final String DEFAULT_MAX_TO_TOTAL = "500";
  public static final String DEFAULT_MAX_IDLE = "300";
  public static final String DEFAULT_MIN_IDLE = "10";
  public static final String DEFAULT_MAX_WAIT_MILLIS = "60000";
  public static final String DEFAULT_TEST_ON_BORROW = "true";
  public static final String DEFAULT_TEST_ON_RETURN = "true";
  public static final String DEFAULT_TEST_WHILE_IDLE = "true";
  public static final String DEFAULT_TIME_BETWEEN_EVICTION_RUNMILLIS = "30000";
  public static final String DEFAULT_NUM_TESTS_PER_EVICTION_RUN = "10";
  public static final String DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLS = "60000";
  public static final String DEFAULT_CLUSTER_MAX_ATTEMP = "1";

  public static final String PARSE_AS_FLUME_EVENT = "parseAsFlumeEvent";
  public static final boolean DEFAULT_PARSE_AS_FLUME_EVENT = true;
}
