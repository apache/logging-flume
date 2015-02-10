/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.hive;

public class Config {
  public static final String HIVE_METASTORE = "hive.metastore";
  public static final String HIVE_DATABASE = "hive.database";
  public static final String HIVE_TABLE = "hive.table";
  public static final String HIVE_PARTITION = "hive.partition";
  public static final String HIVE_TXNS_PER_BATCH_ASK = "hive.txnsPerBatchAsk";
  public static final String BATCH_SIZE = "batchSize";
  public static final String IDLE_TIMEOUT = "idleTimeout";
  public static final String CALL_TIMEOUT = "callTimeout";
  public static final String HEART_BEAT_INTERVAL = "heartBeatInterval";
  public static final String MAX_OPEN_CONNECTIONS = "maxOpenConnections";
  public static final String USE_LOCAL_TIME_STAMP = "useLocalTimeStamp";
  public static final String TIME_ZONE = "timeZone";
  public static final String ROUND_UNIT = "roundUnit";
  public static final String ROUND = "round";
  public static final String HOUR = "hour";
  public static final String MINUTE = "minute";
  public static final String SECOND = "second";
  public static final String ROUND_VALUE = "roundValue";
  public static final String SERIALIZER = "serializer";
}
