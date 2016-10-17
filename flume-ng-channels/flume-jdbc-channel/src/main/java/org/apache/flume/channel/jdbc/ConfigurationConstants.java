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
package org.apache.flume.channel.jdbc;

/**
 * Contains configuration keys used by the JDBC channel implementation.
 */
public final class ConfigurationConstants {

  public static final String PREFIX = "org.apache.flume.channel.jdbc.";

  public static final String CONFIG_JDBC_SYSPROP_PREFIX = "sysprop.";

  /**
   * @deprecated use {@link #CONFIG_JDBC_SYSPROP_PREFIX} instead
   */
  public static final String OLD_CONFIG_JDBC_SYSPROP_PREFIX =
      PREFIX + CONFIG_JDBC_SYSPROP_PREFIX;

  public static final String CONFIG_JDBC_DRIVER_CLASS = "driver.class";

  /**
   * @deprecated use {@link #CONFIG_JDBC_DRIVER_CLASS} instead.
   */
  public static final String OLD_CONFIG_JDBC_DRIVER_CLASS =
      PREFIX + CONFIG_JDBC_DRIVER_CLASS;

  public static final String CONFIG_USERNAME = "db.username";

  /**
   * @deprecated use {@link #CONFIG_USERNAME} instead.
   */
  public static final String OLD_CONFIG_USERNAME =
      PREFIX + CONFIG_USERNAME;

  public static final String CONFIG_PASSWORD = "db.password";

  /**
   * @deprecated use {@link #CONFIG_PASSWORD} instead.
   */
  public static final String OLD_CONFIG_PASSWORD =
      PREFIX + CONFIG_PASSWORD;

  public static final String CONFIG_URL = "driver.url";

  /**
   * @deprecated use {@link #CONFIG_URL} instead.
   */
  public static final String OLD_CONFIG_URL =
      PREFIX + CONFIG_URL;

  public static final String CONFIG_JDBC_PROPS_FILE =
      "connection.properties.file";

  /**
   * @deprecated use {@link #CONFIG_JDBC_PROPS_FILE} instead.
   */
  public static final String OLD_CONFIG_JDBC_PROPS_FILE =
      PREFIX + CONFIG_JDBC_PROPS_FILE;

  public static final String CONFIG_DATABASE_TYPE = "db.type";

  /**
   * @deprecated use {@link #CONFIG_DATABASE_TYPE} instead.
   */
  public static final String OLD_CONFIG_DATABASE_TYPE =
      PREFIX + CONFIG_DATABASE_TYPE;

  public static final String CONFIG_CREATE_SCHEMA = "create.schema";

  /**
   * @deprecated use {@link #CONFIG_CREATE_SCHEMA} instead.
   */
  public static final String OLD_CONFIG_CREATE_SCHEMA =
      PREFIX + CONFIG_CREATE_SCHEMA;

  public static final String CONFIG_CREATE_INDEX = "create.index";

  /**
   * @deprecated use {@link #CONFIG_CREATE_INDEX} instead.
   */
  public static final String OLD_CONFIG_CREATE_INDEX =
      PREFIX + CONFIG_CREATE_INDEX;

  public static final String CONFIG_CREATE_FK = "create.foreignkey";

  /**
   * @deprecated use {@link #CONFIG_CREATE_FK} instead.
   */
  public static final String OLD_CONFIG_CREATE_FK =
      PREFIX + CONFIG_CREATE_FK;

  public static final String CONFIG_TX_ISOLATION_LEVEL =
      "transaction.isolation";

  /**
   * @deprecated use {@link #CONFIG_TX_ISOLATION_LEVEL} instead.
   */
  public static final String OLD_CONFIG_TX_ISOLATION_LEVEL =
      PREFIX + CONFIG_TX_ISOLATION_LEVEL;

  public static final String CONFIG_MAX_CONNECTIONS = "maximum.connections";

  /**
   * @deprecated use {@link #CONFIG_MAX_CONNECTIONS} instead
   */
  public static final String OLD_CONFIG_MAX_CONNECTIONS =
      PREFIX + CONFIG_MAX_CONNECTIONS;

  public static final String CONFIG_MAX_CAPACITY = "maximum.capacity";

  /**
   * @deprecated use {@link #CONFIG_MAX_CAPACITY} instead.
   */
  public static final String OLD_CONFIG_MAX_CAPACITY =
      PREFIX + CONFIG_MAX_CAPACITY;


  // Built in constants for JDBC Channel implementation

  /**
   * The length for payload bytes that will be stored inline. Payloads larger
   * than this length will spill into BLOB.
   */
  public static int PAYLOAD_LENGTH_THRESHOLD = 16384; // 16kb

  /**
   * The length of header name in bytes that will be stored inline. Header
   * names longer than this number will spill over into CLOB.
   */
  public static int HEADER_NAME_LENGTH_THRESHOLD = 251;

  /**
   * The length of header value in bytes that will be stored inline. Header
   * values longer than this number will spill over into CLOB.
   */
  public static int HEADER_VALUE_LENGTH_THRESHOLD = 251;

  /**
   * The maximum length of channel name.
   */
  public static int CHANNEL_NAME_MAX_LENGTH = 64;

  /**
   * The maximum spill size for header names. Together with the value of
   * HEADER_NAME_LENGTH_THRESHOLD this adds up to 32kb.
   */
  public static int HEADER_NAME_SPILL_MAX_LENGTH = 32517;

  /**
   * The maximum spill size for header values. Together with the value of
   * HEADER_VALUE_LENGTH_THRESHOLD, this adds up to 32kb.
   */
  public static int HEADER_VALUE_SPILL_MAX_LENGTH = 32517;

  private ConfigurationConstants() {
    // Disable object creation
  }
}
