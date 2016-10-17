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
package org.apache.flume.channel.jdbc.impl;

import javax.sql.DataSource;

import org.apache.flume.channel.jdbc.DatabaseType;
import org.apache.flume.channel.jdbc.JdbcChannelException;

/**
 * <p>A factory for SchemaHandlers.</p>
 */
public final class SchemaHandlerFactory {

  public static SchemaHandler getHandler(DatabaseType dbType, DataSource dataSource) {
    SchemaHandler handler = null;
    switch (dbType) {
      case DERBY:
        handler = new DerbySchemaHandler(dataSource);
        break;
      case MYSQL:
        handler = new MySQLSchemaHandler(dataSource);
        break;
      default:
        throw new JdbcChannelException("Database " + dbType + " not supported yet");
    }

    return handler;
  }

  private SchemaHandlerFactory() {
    // Disable explicit object creation
  }
}
