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

  public static final String CONFIG_JDBC_SYSPRO_PREFIX =
      PREFIX + "sysprop.";

  public static final String CONFIG_JDBC_DRIVER_CLASS =
      PREFIX + "driver.class";

  public static final String CONFIG_USERNAME =
      PREFIX + "db.username";

  public static final String CONFIG_PASSWORD =
      PREFIX + "db.password";

  public static final String CONFIG_URL =
      PREFIX + "driver.url";

  public static final String CONFIG_JDBC_PROPERTIES_FILE =
      PREFIX + "connection.properties.file";

  public static final String CONFIG_DATABASE_TYPE =
      PREFIX + "db.type";

  public static final String CONFIG_CREATE_SCHEMA =
      PREFIX + "create.schema";

  public static final String CONFIG_CREATE_INDEX =
      PREFIX + "create.index";

  public static final String CONFIG_TX_ISOLATION_LEVEL =
      PREFIX + "transaction.isolation";

  public static final String CONFIG_MAX_CONNECTION =
      PREFIX + "maximum.connections";

  public static final String CONFIG_MAX_CAPACITY =
      PREFIX + "maximum.capacity";

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
