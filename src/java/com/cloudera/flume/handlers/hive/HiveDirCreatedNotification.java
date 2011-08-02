/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.handlers.hive;

import java.util.Collections;
import java.util.Map;

import com.cloudera.flume.conf.FlumeConfiguration;

/**
 * This object provides all of the information required to notify the hive
 * metastore of that a new directory has appeared
 */
public class HiveDirCreatedNotification {
  // hive specific variables
  final String host, user, pw;
  final int port;

  // hive table name and partitioning key/value metadata
  final String dir; // new dir added
  final String table; // table that the dir should be added as a partition to
  final Map<String, String> meta; // partition key-value metadata

  /**
   * Create a hive notification without any defaults
   */
  public HiveDirCreatedNotification(String host, int port, String user,
      String pw, String table, String dir, Map<String, String> meta) {
    this.host = host;
    this.port = port;
    this.user = user;
    this.pw = pw;
    this.table = table;
    this.dir = dir;
    this.meta = meta;
  }

  /**
   * Create hive notification using defaults from FlumeConfiguration.
   */
  public HiveDirCreatedNotification(String table, String dir,
      Map<String, String> meta) {
    FlumeConfiguration conf = FlumeConfiguration.get();
    this.host = conf.getHiveHost();
    this.port = conf.getHivePort();
    this.user = conf.getHiveUser();
    this.pw = conf.getHiveUserPW();
    this.table = table;
    this.dir = dir;
    this.meta = meta;
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getUser() {
    return user;
  }

  public String getPW() {
    return pw;
  }

  public String getNotifDir() {
    return dir;
  }

  public Map<String, String> getHivePartitionInfo() {
    return Collections.unmodifiableMap(meta);
  }

  /**
   * Generate a short hand for the data in the form:
   * 
   * hive://user:pw@hostname:port/tablename notifDir { metadata }
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("hive://");
    sb.append(getUser());
    sb.append(":");
    sb.append(getPW());
    sb.append("@");
    sb.append(getHost());
    sb.append(":");
    sb.append(getPort());

    sb.append("/");
    sb.append(table);

    sb.append(" ");
    sb.append(getNotifDir());
    sb.append(" ");

    sb.append(getHivePartitionInfo());
    return sb.toString();
  }

}
