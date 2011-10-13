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

import org.apache.flume.channel.jdbc.ConfigurationConstants;
import org.apache.flume.channel.jdbc.JdbcChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Schema handler for Derby Database. This handler uses the following schema:
 * </p>
 *
 * <p><strong><tt>FL_EVENT</tt></strong>: The main event table. This table
 * contains an auto-generated event ID along with the first 16kb of payload
 * data. If the payload is larger than 16kb, a spill indicator flag is set and
 * the remaining data is recorded in the <tt>FL_PLSPILL</tt> table.</p>
 * <pre>
 * +-------------------------------+
 * | FL_EVENT                      |
 * +-------------------------------+
 * | FLE_ID     : BIGINT PK        | (auto-gen sequence)
 * | FLE_PAYLOAD: VARBINARY(16384) | (16kb payload)
 * | FLE_SPILL  : BOOLEAN          | (true if payload spills)
 * | FLE_CHANNEL: VARCHAR(64)      |
 * +-------------------------------+
 * </pre>
 *
 * <p><strong><tt>FL_PLSPILL</tt></strong>: This table holds payloads in excess
 * of 16kb and relates back to the <tt>FL_EVENT</tt> table using foreign key
 * reference via <tt>FLP_EVENT</tt> column.</p>
 * <pre>
 * +---------------------+
 * | FL_PLSPILL          |
 * +---------------------+
 * | FLP_EVENT  : BIGINT | (FK into FL_EVENT.FLE_ID)
 * | FLP_SPILL  : BLOB   |
 * +---------------------+
 * </pre>
 * <p><strong><tt>FL_HEADER</tt></strong>: The table that holds headers. This
 * table contains name value pairs of headers less than or up to first 255
 * bytes each. If a name is longer than 255 bytes, a spill indicator flag is
 * set and the remaining bytes are recorded in <tt>FL_NMSPILL</tt> table.
 * Similarly if the value is longer than 255 bytes, a spill indicator flag is
 * set and the remaining bytes are recorded in <tt>FL_VLSPILL</tt> table. Each
 * header record relates back to the <tt>FL_EVENT</tt> table using foreign key
 * reference via <tt>FLH_EVENT</tt> column.</p>
 * <pre>
 * +--------------------------+
 * | FL_HEADER                |
 * +--------------------------+
 * | FLH_ID     : BIGINT PK   | (auto-gen sequence)
 * | FLH_EVENT  : BIGINT      | (FK into FL_EVENT.FLE_ID)
 * | FLH_NAME   : VARCHAR(251)|
 * | FLH_VALUE  : VARCHAR(251)|
 * | FLH_NMSPILL: BOOLEAN     | (true if name spills)
 * | FLH_VLSPILL: BOOLEAN     | (true if value spills)
 * +--------------------------+
 * </pre>
 * <p><strong><tt>FL_NMSPILL</tt></strong>: The table that holds header names
 * in excess of 255 bytes and relates back to the <tt>FL_HEADER</tt> table
 * using foreign key reference via <tt>FLN_HEADER</tt> column.</p>
 * <pre>
 * +----------------------+
 * | FL_NMSPILL           |
 * +----------------------+
 * | FLN_HEADER  : BIGINT | (FK into FL_HEADER.FLH_ID)
 * | FLN_SPILL   : CLOB   |
 * +----------------------+
 * </pre>
 * <p><strong><tt>FL_VLSPILL</tt></strong>: The table that holds header values
 * in excess of 255 bytes and relates back to the <tt>FL_HEADER</tt> table
 * using foreign key reference via <tt>FLV_HEADER</tt> column.</p>
 * <pre>
 * +----------------------+
 * | FL_VLSPILL           |
 * +----------------------+
 * | FLV_HEADER  : BIGINT | (FK into FL_HEADER.FLH_ID)
 * | FLV_SPILL   : CLOB   |
 * +----------------------+
 * </pre>
 * </p>
 * <p><strong>NOTE</strong>: The values that decide the spill boundary
 * and storage length limits are defined in <tt>ConfigurationConstants</tt>
 * class.</p>
 * @see org.apache.flume.channel.jdbc.ConfigurationConstants
 */
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
  private static final String COLUMN_FLE_SPILL = "FLE_SPILL";

  private static final String TABLE_FL_PLSPILL_NAME = "FL_PLSPILL";
  private static final String TABLE_FL_PLSPILL = SCHEMA_FLUME + "."
      + TABLE_FL_PLSPILL_NAME;
  private static final String COLUMN_FLP_EVENT = "FLP_EVENT";
  private static final String COLUMN_FLP_SPILL = "FLP_SPILL";

  private static final String TABLE_FL_HEADER_NAME = "FL_HEADER";
  private static final String TABLE_FL_HEADER = SCHEMA_FLUME + "."
      + TABLE_FL_HEADER_NAME;
  private static final String COLUMN_FLH_ID = "FLH_ID";
  private static final String COLUMN_FLH_EVENT = "FLH_EVENT";
  private static final String COLUMN_FLH_NAME = "FLH_NAME";
  private static final String COLUMN_FLH_VALUE = "FLH_VALUE";
  private static final String COLUMN_FLH_NMSPILL = "FLH_NMSPILL";
  private static final String COLUMN_FLH_VLSPILL = "FLH_VLSPILL";

  private static final String TABLE_FL_NMSPILL_NAME = "FL_NMSPILL";
  private static final String TABLE_FL_NMSPILL = SCHEMA_FLUME + "."
      + TABLE_FL_NMSPILL_NAME;
  private static final String COLUMN_FLN_HEADER = "FLN_HEADER";
  private static final String COLUMN_FLN_SPILL = "FLN_SPILL";

  private static final String TABLE_FL_VLSPILL_NAME = "FL_VLSPILL";
  private static final String TABLE_FL_VLSPILL = SCHEMA_FLUME + "."
      + TABLE_FL_VLSPILL_NAME;
  private static final String COLUMN_FLV_HEADER = "FLV_HEADER";
  private static final String COLUMN_FLV_SPILL = "FLV_SPILL";

  public static final String QUERY_CREATE_SCHEMA_FLUME
      = "CREATE SCHEMA " + SCHEMA_FLUME;

  public static final String QUERY_CREATE_TABLE_FL_EVENT
      = "CREATE TABLE " + TABLE_FL_EVENT + " ( "
        + COLUMN_FLE_ID + " BIGINT GENERATED ALWAYS AS IDENTITY "
        + "(START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
        + COLUMN_FLE_PAYLOAD + " VARCHAR("
        + ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD
        + ") FOR BIT DATA, "
        + COLUMN_FLE_CHANNEL + " VARCHAR("
        + ConfigurationConstants.CHANNEL_NAME_MAX_LENGTH + "), "
        + COLUMN_FLE_SPILL + " BOOLEAN)";

  public static final String QUERY_CREATE_TABLE_FL_PLSPILL
      = "CREATE TABLE " + TABLE_FL_PLSPILL + " ( "
        + COLUMN_FLP_EVENT + " BIGINT, "
        + COLUMN_FLP_SPILL + " BLOB, "
        + "FOREIGN KEY (" + COLUMN_FLP_EVENT + ") REFERENCES "
        + TABLE_FL_EVENT + " (" + COLUMN_FLE_ID + "))";

  public static final String QUERY_CREATE_TABLE_FL_HEADER
      = "CREATE TABLE " + TABLE_FL_HEADER + " ( "
        + COLUMN_FLH_ID + " BIGINT GENERATED ALWAYS AS IDENTITY "
        + "(START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
        + COLUMN_FLH_EVENT + " BIGINT, "
        + COLUMN_FLH_NAME + " VARCHAR("
        + ConfigurationConstants.HEADER_NAME_LENGTH_THRESHOLD + "), "
        + COLUMN_FLH_VALUE + " VARCHAR("
        + ConfigurationConstants.HEADER_VALUE_LENGTH_THRESHOLD + "), "
        + COLUMN_FLH_NMSPILL + " BOOLEAN, "
        + COLUMN_FLH_VLSPILL + " BOOLEAN, "
        + "FOREIGN KEY (" + COLUMN_FLH_EVENT + ") REFERENCES "
        + TABLE_FL_EVENT + " (" + COLUMN_FLE_ID + "))";

  public static final String QUERY_CREATE_TABLE_FL_NMSPILL
      = "CREATE TABLE " + TABLE_FL_NMSPILL + " ( "
        + COLUMN_FLN_HEADER + " BIGINT, "
        + COLUMN_FLN_SPILL + " VARCHAR("
        + ConfigurationConstants.HEADER_NAME_SPILL_MAX_LENGTH + "), "
        + "FOREIGN KEY (" + COLUMN_FLN_HEADER + ") REFERENCES "
        + TABLE_FL_HEADER + " (" + COLUMN_FLH_ID + "))";

  public static final String QUERY_CREATE_TABLE_FL_VLSPILL
      = "CREATE TABLE " + TABLE_FL_VLSPILL + " ( "
        + COLUMN_FLV_HEADER + " BIGINT, "
        + COLUMN_FLV_SPILL + " VARCHAR("
        + ConfigurationConstants.HEADER_VALUE_SPILL_MAX_LENGTH + "), "
        + "FOREIGN KEY (" + COLUMN_FLV_HEADER + ") REFERENCES "
        + TABLE_FL_HEADER + " (" + COLUMN_FLH_ID + "))";

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
    runQuery(QUERY_CREATE_TABLE_FL_PLSPILL);
    runQuery(QUERY_CREATE_TABLE_FL_HEADER);
    runQuery(QUERY_CREATE_TABLE_FL_NMSPILL);
    runQuery(QUERY_CREATE_TABLE_FL_VLSPILL);
  }

  @Override
  public void validateSchema() {
    verifyTableStructure(SCHEMA_FLUME, TABLE_FL_EVENT_NAME,
        COLUMN_FLE_ID, COLUMN_FLE_PAYLOAD, COLUMN_FLE_CHANNEL,
        COLUMN_FLE_SPILL);

    verifyTableStructure(SCHEMA_FLUME, TABLE_FL_PLSPILL_NAME,
        COLUMN_FLP_EVENT, COLUMN_FLP_SPILL);

    verifyTableStructure(SCHEMA_FLUME, TABLE_FL_HEADER_NAME,
        COLUMN_FLH_ID, COLUMN_FLH_EVENT, COLUMN_FLH_NAME, COLUMN_FLH_VALUE,
        COLUMN_FLH_NMSPILL, COLUMN_FLH_VLSPILL);

    verifyTableStructure(SCHEMA_FLUME, TABLE_FL_NMSPILL_NAME,
        COLUMN_FLN_HEADER, COLUMN_FLN_SPILL);

    verifyTableStructure(SCHEMA_FLUME, TABLE_FL_VLSPILL_NAME,
        COLUMN_FLV_HEADER, COLUMN_FLV_SPILL);
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
