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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.flume.channel.jdbc.JdbcChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DerbySchemaHandler implements SchemaHandler {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DerbySchemaHandler.class);


  private static final String QUREY_SYSCHEMA_FLUME
      = "SELECT SCHEMAID FROM SYS.SYSSCHEMAS WHERE SCHEMANAME = 'FLUME'";

  private static final String SCHEMA_FLUME = "FLUME";

  private static final String TABLE_FL_EVENT_NAME = "FL_EVENT";
  private static final String TABLE_FL_EVENT = SCHEMA_FLUME + "."
      + TABLE_FL_EVENT_NAME;
  private static final String COLUMN_FLE_ID = "FLE_ID";
  private static final String COLUMN_FLE_PAYLOAD = "FLE_PAYLOAD";
  private static final String COLUMN_FLE_CHANNEL = "FLE_CHANNEL";

  private static final String TABLE_FL_PLEXT_NAME = "FL_PLEXT";
  private static final String TABLE_FL_PLEXT = SCHEMA_FLUME + "."
      + TABLE_FL_PLEXT_NAME;
  private static final String COLUMN_FLP_EVENTID = "FLP_EVENTID";
  private static final String COLUMN_FLP_SPILL = "FLP_SPILL";

  private static final String TABLE_FL_HEADER_NAME = "FL_HEADER";
  private static final String TABLE_FL_HEADER = SCHEMA_FLUME + "."
      + TABLE_FL_HEADER_NAME;
  private static final String COLUMN_FLH_EVENTID = "FLH_EVENTID";
  private static final String COLUMN_FLH_NAME = "FLH_NAME";
  private static final String COLUMN_FLH_VALUE = "FLH_VALUE";


  public static final String QUERY_CREATE_SCHEMA_FLUME
      = "CREATE SCHEMA " + SCHEMA_FLUME;

  public static final String QUERY_CREATE_TABLE_FL_EVENT
      = "CREATE TABLE " + TABLE_FL_EVENT + " ( "
        + COLUMN_FLE_ID + " BIGINT GENERATED ALWAYS AS IDENTITY "
        + "(START WITH 2, INCREMENT BY 1) PRIMARY KEY, "
        + COLUMN_FLE_PAYLOAD + " VARCHAR(16384) FOR BIT DATA, " // 16kb
        + COLUMN_FLE_CHANNEL + " VARCHAR(32))";

  public static final String QUERY_CREATE_TABLE_FL_PLEXT
      = "CREATE TABLE " + TABLE_FL_PLEXT + " ( "
        + COLUMN_FLP_EVENTID + " BIGINT, "
        + COLUMN_FLP_SPILL + " BLOB, "
        + "FOREIGN KEY (" + COLUMN_FLP_EVENTID + ") REFERENCES "
        + TABLE_FL_EVENT + " (" + COLUMN_FLE_ID + "))";

  public static final String QUERY_CREATE_TABLE_FL_HEADER
      = "CREATE TABLE " + TABLE_FL_HEADER + " ( "
        + COLUMN_FLH_EVENTID + " BIGINT, "
        + COLUMN_FLH_NAME + " VARCHAR(255), "
        + COLUMN_FLH_VALUE + " VARCHAR(255), "
        + "FOREIGN KEY (" + COLUMN_FLH_EVENTID + ") REFERENCES "
        + TABLE_FL_EVENT + " (" + COLUMN_FLE_ID + "))";

  public static final String COLUMN_LOOKUP_QUERY =
      "SELECT COLUMNNAME from SYS.SYSCOLUMNS where REFERENCEID = "
         + "(SELECT TABLEID FROM SYS.SYSTABLES WHERE TABLENAME = ? AND "
         + "SCHEMAID = (SELECT SCHEMAID FROM SYS.SYSSCHEMAS WHERE "
         + "SCHEMANAME = ? ))";

  private final DataSource dataSource;

  protected DerbySchemaHandler(DataSource dataSource) {
    this.dataSource = dataSource;
  }


  @Override
  public boolean schemaExists() {
    Connection connection = null;
    ResultSet rset = null;
    try {
      connection = dataSource.getConnection();
      Statement stmt = connection.createStatement();
      rset = stmt.executeQuery(QUREY_SYSCHEMA_FLUME);
      if (!rset.next()) {
        LOGGER.warn("Schema for FLUME does not exist");
        return false;
      }

      String flumeSchemaId = rset.getString(1);
      LOGGER.debug("Flume schema ID: " + flumeSchemaId);

      connection.commit();
    } catch (SQLException ex) {
      try {
        connection.rollback();
      } catch (SQLException ex2) {
        LOGGER.error("Unable to rollback transaction", ex2);
      }
      throw new JdbcChannelException("Unable to query schema", ex);
    } finally {
      if (rset != null) {
        try {
          rset.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close result set", ex);
        }
      }
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close connection", ex);
        }
      }
    }

    return true;
  }

  @Override
  public void createSchemaObjects() {
    runQuery(QUERY_CREATE_SCHEMA_FLUME);
    runQuery(QUERY_CREATE_TABLE_FL_EVENT);
    runQuery(QUERY_CREATE_TABLE_FL_PLEXT);
    runQuery(QUERY_CREATE_TABLE_FL_HEADER);
  }

  @Override
  public void validateSchema() {
    verifyTableStructure(SCHEMA_FLUME, TABLE_FL_EVENT_NAME,
        COLUMN_FLE_ID, COLUMN_FLE_PAYLOAD, COLUMN_FLE_CHANNEL);

    verifyTableStructure(SCHEMA_FLUME, TABLE_FL_PLEXT_NAME,
        COLUMN_FLP_EVENTID, COLUMN_FLP_SPILL);

    verifyTableStructure(SCHEMA_FLUME, TABLE_FL_HEADER_NAME,
        COLUMN_FLH_EVENTID, COLUMN_FLH_NAME, COLUMN_FLH_VALUE);
  }

  private void verifyTableStructure(String schemaName, String tableName,
      String... columns) {
    Set<String> columnNames = new HashSet<String>();
    Connection connection = null;
    PreparedStatement pStmt = null;
    try {
      connection = dataSource.getConnection();
      pStmt = connection.prepareStatement(COLUMN_LOOKUP_QUERY);
      pStmt.setString(1, tableName);
      pStmt.setString(2, schemaName);
      ResultSet rset = pStmt.executeQuery();

      while (rset.next()) {
        columnNames.add(rset.getString(1));
      }
      connection.commit();
    } catch (SQLException ex) {
      try {
        connection.rollback();
      } catch (SQLException ex2) {
        LOGGER.error("Unable to rollback transaction", ex2);
      }
      throw new JdbcChannelException("Unable to run query: "
          + COLUMN_LOOKUP_QUERY + ": 1=" + tableName + ", 2=" + schemaName, ex);
    } finally {
      if (pStmt != null) {
        try {
          pStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close statement", ex);
        }
        if (connection != null) {
          try {
            connection.close();
          } catch (SQLException ex) {
            LOGGER.error("Unable to close connection", ex);
          }
        }
      }
    }

    Set<String> columnDiff = new HashSet<String>();
    columnDiff.addAll(columnNames);

    // Expected Column string form
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (String column : columns) {
      columnDiff.remove(column);
      if (first) {
        first = false;
      } else {
        sb.append(", ");
      }
      sb.append(column);
    }
    sb.append("}");

    String expectedColumns = sb.toString();

    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Table " + schemaName + "." + tableName
          + " expected columns: " + expectedColumns + ", actual columns: "
          + columnNames);
    }

    if (columnNames.size() != columns.length || columnDiff.size() != 0) {
      throw new JdbcChannelException("Expected table " + schemaName + "."
          + tableName + " to have columns: " + expectedColumns + ". Instead "
          + "found columns: " + columnNames);
    }
  }

  private void runQuery(String query) {
    Connection connection = null;
    Statement stmt = null;
    try {
      connection = dataSource.getConnection();
      stmt = connection.createStatement();
      if (stmt.execute(query)) {
        ResultSet rset = stmt.getResultSet();
        int count = 0;
        while (rset.next()) {
          count++;
        }
        LOGGER.info("QUERY(" + query + ") produced unused resultset with "
            + count + " rows");
      } else {
        int updateCount = stmt.getUpdateCount();
        LOGGER.info("QUERY(" + query + ") Update count: " + updateCount);
      }
      connection.commit();
    } catch (SQLException ex) {
      try {
        connection.rollback();
      } catch (SQLException ex2) {
        LOGGER.error("Unable to rollback transaction", ex2);
      }
      throw new JdbcChannelException("Unable to run query: "
          + query, ex);
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close statement", ex);
        }
        if (connection != null) {
          try {
            connection.close();
          } catch (SQLException ex) {
            LOGGER.error("Unable to close connection", ex);
          }
        }
      }
    }
  }
}
