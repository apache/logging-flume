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
package org.apache.flume.sink.hbase;

import org.apache.hadoop.hbase.HConstants;

/**
 * Constants used for configuration of HBaseSink and AsyncHBaseSink
 *
 */
public class HBaseSinkConfigurationConstants {
  /**
   * The Hbase table which the sink should write to.
   */
  public static final String CONFIG_TABLE = "table";
  /**
   * The column family which the sink should use.
   */
  public static final String CONFIG_COLUMN_FAMILY = "columnFamily";
  /**
   * Maximum number of events the sink should take from the channel per
   * transaction, if available.
   */
  public static final String CONFIG_BATCHSIZE = "batchSize";
  /**
   * The fully qualified class name of the serializer the sink should use.
   */
  public static final String CONFIG_SERIALIZER = "serializer";
  /**
   * Configuration to pass to the serializer.
   */
  public static final String CONFIG_SERIALIZER_PREFIX = CONFIG_SERIALIZER + ".";

  public static final String CONFIG_TIMEOUT = "timeout";

  public static final String CONFIG_ENABLE_WAL = "enableWal";

  public static final boolean DEFAULT_ENABLE_WAL = true;

  public static final long DEFAULT_TIMEOUT = 60000;

  public static final String CONFIG_KEYTAB = "kerberosKeytab";

  public static final String CONFIG_PRINCIPAL = "kerberosPrincipal";

  public static final String ZK_QUORUM = "zookeeperQuorum";

  public static final String ZK_ZNODE_PARENT = "znodeParent";

  public static final String DEFAULT_ZK_ZNODE_PARENT =
      HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT;

  public static final String CONFIG_COALESCE_INCREMENTS = "coalesceIncrements";

  public static final Boolean DEFAULT_COALESCE_INCREMENTS = false;

}
