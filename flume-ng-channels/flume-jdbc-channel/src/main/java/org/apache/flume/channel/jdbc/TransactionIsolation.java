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

import java.sql.Connection;
import java.util.Locale;

public enum TransactionIsolation {

  READ_UNCOMMITTED("READ_UNCOMMITTED", Connection.TRANSACTION_READ_UNCOMMITTED),
  READ_COMMITTED("READ_COMMITTED", Connection.TRANSACTION_READ_COMMITTED),
  REPEATABLE_READ("REPEATABLE_READ", Connection.TRANSACTION_REPEATABLE_READ),
  SERIALIZABLE("SERIALIZABLE", Connection.TRANSACTION_SERIALIZABLE);

  private final String name;
  private final int code;

  private TransactionIsolation(String name, int code) {
    this.name = name;
    this.code = code;
  }

  public int getCode() {
    return code;
  }

  public String getName() {
    return name;
  }

  public String toString() {
    return getName();
  }

  public static TransactionIsolation getByName(String name) {
    return valueOf(name.trim().toUpperCase(Locale.ENGLISH));
  }
}
