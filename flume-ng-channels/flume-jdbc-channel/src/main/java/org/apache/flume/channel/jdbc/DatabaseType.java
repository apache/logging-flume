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

import java.util.Locale;

public enum DatabaseType {
  /** All other databases */
  OTHER("OTHER", null),

  /** Apache Derby */
  DERBY("DERBY", "values(1)"),

  /** MySQL */
  MYSQL("MYSQL", "select 1"),

  /** PostgreSQL */
  POSTGRESQL("POSTGRESQL", null),

  /** Oracle */
  ORACLE("ORACLE", null);

  private final String name;
  private final String validationQuery;

  private DatabaseType(String name, String validationQuery) {
    this.name = name;
    this.validationQuery = validationQuery;
  }

  public String toString() {
    return getName();
  }

  public String getName() {
    return name;
  }

  public String getValidationQuery() {
    return validationQuery;
  }

  public static DatabaseType getByName(String dbName) {
    DatabaseType type = null;
    try {
      type = DatabaseType.valueOf(dbName.trim().toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException ex) {
      type = DatabaseType.OTHER;
    }

    return type;
  }

}
