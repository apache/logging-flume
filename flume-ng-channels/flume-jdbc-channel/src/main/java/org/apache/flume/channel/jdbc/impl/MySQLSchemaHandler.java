/*
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

import javax.sql.DataSource;

public class MySQLSchemaHandler implements SchemaHandler {

  private final DataSource dataSource;

  protected MySQLSchemaHandler(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public boolean schemaExists() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void validateSchema() {
    // TODO Auto-generated method stub

  }

  @Override
  public void storeEvent(PersistableEvent pe, Connection connection) {
    // TODO Auto-generated method stub
  }

  @Override
  public PersistableEvent fetchAndDeleteEvent(String channel,
      Connection connection) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getChannelSize(Connection connection) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void createSchemaObjects(boolean createForeignKeys, boolean createIndex) {
    // TODO Auto-generated method stub

  }
}
