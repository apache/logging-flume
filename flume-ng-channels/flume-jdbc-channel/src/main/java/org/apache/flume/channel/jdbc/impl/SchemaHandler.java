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

import java.sql.Connection;

/**
 * <p>A handler for creating and validating database schema for use by
 * the JDBC channel implementation.</p>
 */
public interface SchemaHandler {

  /**
   * @param connection the connection to check for schema.
   * @return true if the schema exists. False otherwise.
   */
  public boolean schemaExists();

  /**
   * Validates the schema.
   * @param connection
   */
  public void validateSchema();

  /**
   * Creates the schema.
   * @param connection the connection to create schema for.
   */
  public void createSchemaObjects();

  /**
   * Inserts the given persistent event into the database. The connection that
   * is passed into the handler has an ongoing transaction and therefore the
   * SchemaHandler implementation must not close the connection.
   *
   * @param pe the event to persist
   * @param connection the connection to use
   */
  public void persistEvent(PersistableEvent pe, Connection connection);
}
