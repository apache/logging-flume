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

  public static final String CONFIG_DRIVER_CLASS =
      PREFIX + "driver.class";

  public static final String CONFIG_DATABASE_TYPE =
      PREFIX + "db.type";

  public static final String CONFIG_USERNAME =
      PREFIX + "db.username";

  public static final String CONFIG_PASSWORD =
      PREFIX + "db.password";

  public static final String CONFIG_URL =
      PREFIX + "driver.url";

  public static final String CONFIG_JDBC_PROPERTIES_FILE =
      PREFIX + "properties.file";

  public static final String CONFIG_CREATE_SCHEMA =
      PREFIX + "create.schema";

  private ConfigurationConstants() {
    // Disable object creation
  }
}
