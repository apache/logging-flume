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
   * @param createForeignKeys a flag which indicates if the foreign key
   *        constraints should be created where necessary.
   * @param createIndex a flag which indicates if indexes must be created during
   *        the creation of the schema.
   */
  public void createSchemaObjects(boolean createForeignKeys,
      boolean createIndex);

  /**
   * Inserts the given persistent event into the database. The connection that
   * is passed into the handler has an ongoing transaction and therefore the
   * SchemaHandler implementation must not close the connection.
   *
   * @param pe the event to persist
   * @param connection the connection to use
   */
  public void storeEvent(PersistableEvent pe, Connection connection);

  /**
   * Retrieves the next persistent event from the database. The connection that
   * is passed into the handler has an ongoing transaction and therefore the
   * SchemaHandler implementation must not close the connection.
   *
   * @param channel the channel name from which event will be retrieved
   * @param connection the connection to use
   * @return the next persistent event if available or null
   */
  public PersistableEvent fetchAndDeleteEvent(
      String channel, Connection connection);

  /**
   * Returns the current size of the channel using the connection specified that
   * must have an active transaction ongoing. This allows the provider impl to
   * enforce channel capacity limits when persisting events.
   * @return the current size of the channel.
   * @param connection
   * @return
   */
  public long getChannelSize(Connection connection);
}
