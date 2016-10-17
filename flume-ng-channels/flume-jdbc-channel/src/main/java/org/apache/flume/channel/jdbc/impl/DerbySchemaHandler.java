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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.flume.channel.jdbc.ConfigurationConstants;
import org.apache.flume.channel.jdbc.JdbcChannelException;
import org.apache.flume.channel.jdbc.impl.PersistableEvent.HeaderEntry;
import org.apache.flume.channel.jdbc.impl.PersistableEvent.SpillableString;
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
  private static final String INDEX_FLE_CHANNEL_NAME = "IDX_FLE_CHANNEL";
  private static final String INDEX_FLE_CHANNEL = SCHEMA_FLUME + "."
      + INDEX_FLE_CHANNEL_NAME;

  private static final String TABLE_FL_PLSPILL_NAME = "FL_PLSPILL";
  private static final String TABLE_FL_PLSPILL = SCHEMA_FLUME + "."
      + TABLE_FL_PLSPILL_NAME;
  private static final String COLUMN_FLP_EVENT = "FLP_EVENT";
  private static final String COLUMN_FLP_SPILL = "FLP_SPILL";
  private static final String INDEX_FLP_EVENT_NAME = "IDX_FLP_EVENT";
  private static final String INDEX_FLP_EVENT = SCHEMA_FLUME + "."
      + INDEX_FLP_EVENT_NAME;

  private static final String TABLE_FL_HEADER_NAME = "FL_HEADER";
  private static final String TABLE_FL_HEADER = SCHEMA_FLUME + "."
      + TABLE_FL_HEADER_NAME;
  private static final String COLUMN_FLH_ID = "FLH_ID";
  private static final String COLUMN_FLH_EVENT = "FLH_EVENT";
  private static final String COLUMN_FLH_NAME = "FLH_NAME";
  private static final String COLUMN_FLH_VALUE = "FLH_VALUE";
  private static final String COLUMN_FLH_NMSPILL = "FLH_NMSPILL";
  private static final String COLUMN_FLH_VLSPILL = "FLH_VLSPILL";
  private static final String INDEX_FLH_EVENT_NAME = "IDX_FLH_EVENT";
  private static final String INDEX_FLH_EVENT = SCHEMA_FLUME + "."
      + INDEX_FLH_EVENT_NAME;

  private static final String TABLE_FL_NMSPILL_NAME = "FL_NMSPILL";
  private static final String TABLE_FL_NMSPILL = SCHEMA_FLUME + "."
      + TABLE_FL_NMSPILL_NAME;
  private static final String COLUMN_FLN_HEADER = "FLN_HEADER";
  private static final String COLUMN_FLN_SPILL = "FLN_SPILL";
  private static final String INDEX_FLN_HEADER_NAME = "IDX_FLN_HEADER";
  private static final String INDEX_FLN_HEADER = SCHEMA_FLUME + "."
      + INDEX_FLN_HEADER_NAME;

  private static final String TABLE_FL_VLSPILL_NAME = "FL_VLSPILL";
  private static final String TABLE_FL_VLSPILL = SCHEMA_FLUME + "."
      + TABLE_FL_VLSPILL_NAME;
  private static final String COLUMN_FLV_HEADER = "FLV_HEADER";
  private static final String COLUMN_FLV_SPILL = "FLV_SPILL";
  private static final String INDEX_FLV_HEADER_NAME = "IDX_FLV_HEADER";
  private static final String INDEX_FLV_HEADER = SCHEMA_FLUME + "."
      + INDEX_FLV_HEADER_NAME;

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

  public static final String QUERY_CREATE_INDEX_FLE_CHANNEL
      = "CREATE INDEX " + INDEX_FLE_CHANNEL + " ON " + TABLE_FL_EVENT
        + " (" + COLUMN_FLE_CHANNEL + ")";

  public static final String QUERY_CREATE_TABLE_FL_PLSPILL_FMT
      = "CREATE TABLE " + TABLE_FL_PLSPILL + " ( "
        + COLUMN_FLP_EVENT + " BIGINT, "
        + COLUMN_FLP_SPILL + " BLOB{0})";

  public static final String QUERY_CREATE_TABLE_FL_PLSPILL_FK
      = MessageFormat.format(QUERY_CREATE_TABLE_FL_PLSPILL_FMT,
          ", FOREIGN KEY (" + COLUMN_FLP_EVENT + ") REFERENCES "
              + TABLE_FL_EVENT + " (" + COLUMN_FLE_ID + ")"
          );

  public static final String QUERY_CREATE_TABLE_FL_PLSPILL_NOFK
      = MessageFormat.format(QUERY_CREATE_TABLE_FL_PLSPILL_FMT, "");

  public static final String QUERY_CREATE_INDEX_FLP_EVENT
      = "CREATE INDEX " + INDEX_FLP_EVENT + " ON " + TABLE_FL_PLSPILL
        + " (" + COLUMN_FLP_EVENT + ")";

  public static final String QUERY_CREATE_TABLE_FL_HEADER_FMT
      = "CREATE TABLE " + TABLE_FL_HEADER + " ( "
        + COLUMN_FLH_ID + " BIGINT GENERATED ALWAYS AS IDENTITY "
        + "(START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
        + COLUMN_FLH_EVENT + " BIGINT, "
        + COLUMN_FLH_NAME + " VARCHAR("
        + ConfigurationConstants.HEADER_NAME_LENGTH_THRESHOLD + "), "
        + COLUMN_FLH_VALUE + " VARCHAR("
        + ConfigurationConstants.HEADER_VALUE_LENGTH_THRESHOLD + "), "
        + COLUMN_FLH_NMSPILL + " BOOLEAN, "
        + COLUMN_FLH_VLSPILL + " BOOLEAN{0})";

  public static final String QUERY_CREATE_TABLE_FL_HEADER_FK
      = MessageFormat.format(QUERY_CREATE_TABLE_FL_HEADER_FMT,
          ", FOREIGN KEY (" + COLUMN_FLH_EVENT + ") REFERENCES "
              + TABLE_FL_EVENT + " (" + COLUMN_FLE_ID + ")"
        );

  public static final String QUERY_CREATE_TABLE_FL_HEADER_NOFK
      = MessageFormat.format(QUERY_CREATE_TABLE_FL_HEADER_FMT, "");

  public static final String QUERY_CREATE_INDEX_FLH_EVENT
      = "CREATE INDEX " + INDEX_FLH_EVENT + " ON " + TABLE_FL_HEADER
         + " (" + COLUMN_FLH_EVENT + ")";

  public static final String QUERY_CREATE_TABLE_FL_NMSPILL_FMT
      = "CREATE TABLE " + TABLE_FL_NMSPILL + " ( "
         + COLUMN_FLN_HEADER + " BIGINT, "
         + COLUMN_FLN_SPILL + " VARCHAR("
         + ConfigurationConstants.HEADER_NAME_SPILL_MAX_LENGTH + "){0})";

  public static final String QUERY_CREATE_TABLE_FL_NMSPILL_FK
      = MessageFormat.format(QUERY_CREATE_TABLE_FL_NMSPILL_FMT,
          ", FOREIGN KEY (" + COLUMN_FLN_HEADER + ") REFERENCES "
              + TABLE_FL_HEADER + " (" + COLUMN_FLH_ID + ")"
        );

  public static final String QUERY_CREATE_TABLE_FL_NMSPILL_NOFK
      = MessageFormat.format(QUERY_CREATE_TABLE_FL_NMSPILL_FMT, "");

  public static final String QUERY_CREATE_INDEX_FLN_HEADER
      = "CREATE INDEX " + INDEX_FLN_HEADER + " ON " + TABLE_FL_NMSPILL
         + " (" + COLUMN_FLN_HEADER + ")";

  public static final String QUERY_CREATE_TABLE_FL_VLSPILL_FMT
      = "CREATE TABLE " + TABLE_FL_VLSPILL + " ( "
        + COLUMN_FLV_HEADER + " BIGINT, "
        + COLUMN_FLV_SPILL + " VARCHAR("
        + ConfigurationConstants.HEADER_VALUE_SPILL_MAX_LENGTH + "){0})";

  public static final String QUERY_CREATE_TABLE_FL_VLSPILL_FK
      = MessageFormat.format(QUERY_CREATE_TABLE_FL_VLSPILL_FMT,
            ", FOREIGN KEY (" + COLUMN_FLV_HEADER + ") REFERENCES "
                + TABLE_FL_HEADER + " (" + COLUMN_FLH_ID + ")"
        );

  public static final String QUERY_CREATE_TABLE_FL_VLSPILL_NOFK
      = MessageFormat.format(QUERY_CREATE_TABLE_FL_VLSPILL_FMT, "");

  public static final String QUERY_CREATE_INDEX_FLV_HEADER
      = "CREATE INDEX " + INDEX_FLV_HEADER + " ON " + TABLE_FL_VLSPILL
         + " (" + COLUMN_FLV_HEADER + ")";

  public static final String COLUMN_LOOKUP_QUERY
      = "SELECT COLUMNNAME from SYS.SYSCOLUMNS where REFERENCEID = "
         + "(SELECT TABLEID FROM SYS.SYSTABLES WHERE TABLENAME = ? AND "
         + "SCHEMAID = (SELECT SCHEMAID FROM SYS.SYSSCHEMAS WHERE "
         + "SCHEMANAME = ? ))";

  public static final String QUERY_CHANNEL_SIZE
      = "SELECT COUNT(*) FROM " + TABLE_FL_EVENT;

  public static final String STMT_INSERT_EVENT_BASE
      = "INSERT INTO " + TABLE_FL_EVENT + " ("
          + COLUMN_FLE_PAYLOAD + ", " + COLUMN_FLE_CHANNEL + ", "
          + COLUMN_FLE_SPILL + ") VALUES ( ?, ?, ?)";

  public static final String STMT_INSERT_EVENT_SPILL
      = "INSERT INTO " + TABLE_FL_PLSPILL + " ("
          + COLUMN_FLP_EVENT + ", " + COLUMN_FLP_SPILL + ") VALUES ( ?, ?)";

  public static final String STMT_INSERT_HEADER_BASE
      = "INSERT INTO " + TABLE_FL_HEADER + " ("
          + COLUMN_FLH_EVENT + ", " + COLUMN_FLH_NAME + ", " + COLUMN_FLH_VALUE
          + ", " + COLUMN_FLH_NMSPILL + ", " + COLUMN_FLH_VLSPILL + ") VALUES "
          + "( ?, ?, ?, ?, ?)";

  public static final String STMT_INSERT_HEADER_NAME_SPILL
      = "INSERT INTO " + TABLE_FL_NMSPILL + " ("
          + COLUMN_FLN_HEADER + ", " + COLUMN_FLN_SPILL + ") VALUES ( ?, ?)";

  public static final String STMT_INSERT_HEADER_VALUE_SPILL
      = "INSERT INTO " + TABLE_FL_VLSPILL + " ("
          + COLUMN_FLV_HEADER + ", " + COLUMN_FLV_SPILL + ") VALUES ( ?, ?)";

  public static final String STMT_FETCH_PAYLOAD_BASE
      = "SELECT " + COLUMN_FLE_ID + ", " + COLUMN_FLE_PAYLOAD + ", "
          + COLUMN_FLE_SPILL + " FROM " + TABLE_FL_EVENT + " WHERE "
          + COLUMN_FLE_ID + " = (SELECT MIN(" + COLUMN_FLE_ID + ") FROM "
          + TABLE_FL_EVENT + " WHERE " + COLUMN_FLE_CHANNEL + " = ?)";

  public static final String STMT_FETCH_PAYLOAD_SPILL
      = "SELECT " + COLUMN_FLP_SPILL + " FROM " + TABLE_FL_PLSPILL + " WHERE "
          + COLUMN_FLP_EVENT + " = ?";

  public static final String STMT_FETCH_HEADER_BASE
      = "SELECT " + COLUMN_FLH_ID + ", " + COLUMN_FLH_NAME + ", "
          + COLUMN_FLH_VALUE + ", " + COLUMN_FLH_NMSPILL + ", "
          + COLUMN_FLH_VLSPILL + " FROM " + TABLE_FL_HEADER + " WHERE "
          + COLUMN_FLH_EVENT + " = ?";

  public static final String STMT_FETCH_HEADER_NAME_SPILL
      = "SELECT " + COLUMN_FLN_SPILL + " FROM " + TABLE_FL_NMSPILL
          + " WHERE " + COLUMN_FLN_HEADER + " = ?";

  public static final String STMT_FETCH_HEADER_VALUE_SPILL
      = "SELECT " + COLUMN_FLV_SPILL + " FROM " + TABLE_FL_VLSPILL
          + " WHERE " + COLUMN_FLV_HEADER + " = ?";

  public static final String STMT_DELETE_HEADER_VALUE_SPILL
      = "DELETE FROM " + TABLE_FL_VLSPILL + " WHERE "
          + COLUMN_FLV_HEADER + " = ?";

  public static final String STMT_DELETE_HEADER_NAME_SPILL
      = "DELETE FROM " + TABLE_FL_NMSPILL + " WHERE "
          + COLUMN_FLN_HEADER + " = ?";

  public static final String STMT_DELETE_EVENT_SPILL
      = "DELETE FROM " + TABLE_FL_PLSPILL + " WHERE "
          + COLUMN_FLP_EVENT + " = ?";

  public static final String STMT_DELETE_HEADER_BASE
      = "DELETE FROM " + TABLE_FL_HEADER + " WHERE "
          + COLUMN_FLH_EVENT + " = ?";

  public static final String STMT_DELETE_EVENT_BASE
      = "DELETE FROM " + TABLE_FL_EVENT + " WHERE "
          + COLUMN_FLE_ID + " = ?";

  private final DataSource dataSource;

  protected DerbySchemaHandler(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public boolean schemaExists() {
    Connection connection = null;
    Statement stmt = null;
    try {
      connection = dataSource.getConnection();
      stmt = connection.createStatement();
      ResultSet  rset = stmt.executeQuery(QUREY_SYSCHEMA_FLUME);
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
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close schema lookup stmt", ex);
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
  public void createSchemaObjects(boolean createForeignKeys,
      boolean createIndex) {
    runQuery(QUERY_CREATE_SCHEMA_FLUME);
    runQuery(QUERY_CREATE_TABLE_FL_EVENT);

    if (createForeignKeys) {
      runQuery(QUERY_CREATE_TABLE_FL_PLSPILL_FK);
      runQuery(QUERY_CREATE_TABLE_FL_HEADER_FK);
      runQuery(QUERY_CREATE_TABLE_FL_NMSPILL_FK);
      runQuery(QUERY_CREATE_TABLE_FL_VLSPILL_FK);
    } else {
      runQuery(QUERY_CREATE_TABLE_FL_PLSPILL_NOFK);
      runQuery(QUERY_CREATE_TABLE_FL_HEADER_NOFK);
      runQuery(QUERY_CREATE_TABLE_FL_NMSPILL_NOFK);
      runQuery(QUERY_CREATE_TABLE_FL_VLSPILL_NOFK);
    }

    if (createIndex) {
      runQuery(QUERY_CREATE_INDEX_FLE_CHANNEL);
      runQuery(QUERY_CREATE_INDEX_FLH_EVENT);
      runQuery(QUERY_CREATE_INDEX_FLP_EVENT);
      runQuery(QUERY_CREATE_INDEX_FLN_HEADER);
      runQuery(QUERY_CREATE_INDEX_FLV_HEADER);
    }
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

  @Override
  public void storeEvent(PersistableEvent pe, Connection connection) {
    // First populate the main event table
    byte[] basePayload = pe.getBasePayload();
    byte[] spillPayload = pe.getSpillPayload();
    boolean hasSpillPayload = (spillPayload != null);
    String channelName = pe.getChannelName();

    LOGGER.debug("Preparing insert event: " + pe);

    PreparedStatement baseEventStmt = null;
    PreparedStatement spillEventStmt = null;
    PreparedStatement baseHeaderStmt = null;
    PreparedStatement headerNameSpillStmt = null;
    PreparedStatement headerValueSpillStmt = null;
    try {
      baseEventStmt = connection.prepareStatement(STMT_INSERT_EVENT_BASE,
                          Statement.RETURN_GENERATED_KEYS);
      baseEventStmt.setBytes(1, basePayload);
      baseEventStmt.setString(2, channelName);
      baseEventStmt.setBoolean(3, hasSpillPayload);

      int baseEventCount = baseEventStmt.executeUpdate();
      if (baseEventCount != 1) {
        throw new JdbcChannelException("Invalid update count on base "
            + "event insert: " + baseEventCount);
      }
      // Extract event ID and set it
      ResultSet eventIdResult = baseEventStmt.getGeneratedKeys();

      if (!eventIdResult.next()) {
        throw new JdbcChannelException("Unable to retrieive inserted event-id");
      }

      long eventId = eventIdResult.getLong(1);
      pe.setEventId(eventId);

      // Persist the payload spill
      if (hasSpillPayload) {
        spillEventStmt = connection.prepareStatement(STMT_INSERT_EVENT_SPILL);
        spillEventStmt.setLong(1, eventId);
        spillEventStmt.setBinaryStream(2,
            new ByteArrayInputStream(spillPayload), spillPayload.length);
        int spillEventCount = spillEventStmt.executeUpdate();
        if (spillEventCount != 1) {
          throw new JdbcChannelException("Invalid update count on spill "
              + "event insert: " + spillEventCount);
        }
      }

      // Persist the headers
      List<HeaderEntry> headers = pe.getHeaderEntries();
      if (headers != null && headers.size() > 0) {
        List<HeaderEntry> headerWithNameSpill = new ArrayList<HeaderEntry>();
        List<HeaderEntry> headerWithValueSpill = new ArrayList<HeaderEntry>();

        baseHeaderStmt = connection.prepareStatement(STMT_INSERT_HEADER_BASE,
                                Statement.RETURN_GENERATED_KEYS);
        Iterator<HeaderEntry> it = headers.iterator();
        while (it.hasNext()) {
          HeaderEntry entry = it.next();
          SpillableString name = entry.getName();
          SpillableString value = entry.getValue();
          baseHeaderStmt.setLong(1, eventId);
          baseHeaderStmt.setString(2, name.getBase());
          baseHeaderStmt.setString(3, value.getBase());
          baseHeaderStmt.setBoolean(4, name.hasSpill());
          baseHeaderStmt.setBoolean(5, value.hasSpill());

          int updateCount = baseHeaderStmt.executeUpdate();
          if (updateCount != 1) {
            throw new JdbcChannelException("Unexpected update header count: " + updateCount);
          }
          ResultSet headerIdResultSet = baseHeaderStmt.getGeneratedKeys();
          if (!headerIdResultSet.next()) {
            throw new JdbcChannelException(
                "Unable to retrieve inserted header id");
          }
          long headerId = headerIdResultSet.getLong(1);
          entry.setId(headerId);

          if (name.hasSpill()) {
            headerWithNameSpill.add(entry);
          }

          if (value.hasSpill()) {
            headerWithValueSpill.add(entry);
          }
        }

        // Persist header name spills
        if (headerWithNameSpill.size() > 0) {
          LOGGER.debug("Number of headers with name spill: "
                  + headerWithNameSpill.size());

          headerNameSpillStmt =
              connection.prepareStatement(STMT_INSERT_HEADER_NAME_SPILL);

          for (HeaderEntry entry : headerWithNameSpill) {
            String nameSpill = entry.getName().getSpill();

            headerNameSpillStmt.setLong(1, entry.getId());
            headerNameSpillStmt.setString(2, nameSpill);
            headerNameSpillStmt.addBatch();
          }

          int[] nameSpillUpdateCount = headerNameSpillStmt.executeBatch();
          if (nameSpillUpdateCount.length != headerWithNameSpill.size()) {
            throw new JdbcChannelException("Unexpected update count for header "
                + "name spills: expected " + headerWithNameSpill.size() + ", "
                + "found " + nameSpillUpdateCount.length);
          }

          for (int i = 0; i < nameSpillUpdateCount.length; i++) {
            if (nameSpillUpdateCount[i] != 1) {
              throw new JdbcChannelException("Unexpected update count for "
                  + "header name spill at position " + i + ", value: "
                  + nameSpillUpdateCount[i]);
            }
          }
        }

        // Persist header value spills
        if (headerWithValueSpill.size() > 0) {
          LOGGER.debug("Number of headers with value spill: "
              + headerWithValueSpill.size());

          headerValueSpillStmt =
              connection.prepareStatement(STMT_INSERT_HEADER_VALUE_SPILL);

          for (HeaderEntry entry : headerWithValueSpill) {
            String valueSpill = entry.getValue().getSpill();

            headerValueSpillStmt.setLong(1, entry.getId());
            headerValueSpillStmt.setString(2, valueSpill);
            headerValueSpillStmt.addBatch();
          }

          int[] valueSpillUpdateCount = headerValueSpillStmt.executeBatch();
          if (valueSpillUpdateCount.length != headerWithValueSpill.size()) {
            throw new JdbcChannelException("Unexpected update count for header "
                + "value spills: expected " + headerWithValueSpill.size() + ", "
                + "found " + valueSpillUpdateCount.length);
          }

          for (int i = 0; i < valueSpillUpdateCount.length; i++) {
            if (valueSpillUpdateCount[i] != 1) {
              throw new JdbcChannelException("Unexpected update count for "
                  + "header value spill at position " + i + ", value: "
                  + valueSpillUpdateCount[i]);
            }
          }
        }
      }
    } catch (SQLException ex) {
      throw new JdbcChannelException("Unable to persist event: " + pe, ex);
    } finally {
      if (baseEventStmt != null) {
        try {
          baseEventStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close base event statement", ex);
        }
      }
      if (spillEventStmt != null) {
        try {
          spillEventStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close spill event statement", ex);
        }
      }
      if (baseHeaderStmt != null) {
        try {
          baseHeaderStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close base header statement", ex);
        }
      }
      if (headerNameSpillStmt != null) {
        try {
          headerNameSpillStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close header name spill statement", ex);
        }
      }
      if (headerValueSpillStmt != null) {
        try {
          headerValueSpillStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close header value spill statement", ex);
        }
      }
    }

    LOGGER.debug("Event persisted: " + pe);
  }

  @Override
  public PersistableEvent fetchAndDeleteEvent(String channel,
      Connection connection) {
    PersistableEvent.Builder peBuilder = null;
    PreparedStatement baseEventFetchStmt = null;
    PreparedStatement spillEventFetchStmt = null;
    InputStream payloadInputStream = null;
    PreparedStatement baseHeaderFetchStmt = null;
    PreparedStatement nameSpillHeaderStmt = null;
    PreparedStatement valueSpillHeaderStmt = null;
    PreparedStatement deleteSpillEventStmt = null;
    PreparedStatement deleteNameSpillHeaderStmt = null;
    PreparedStatement deleteValueSpillHeaderStmt = null;
    PreparedStatement deleteBaseHeaderStmt = null;
    PreparedStatement deleteBaseEventStmt = null;
    try {
      baseEventFetchStmt = connection.prepareStatement(STMT_FETCH_PAYLOAD_BASE);
      baseEventFetchStmt.setString(1, channel);
      ResultSet rsetBaseEvent = baseEventFetchStmt.executeQuery();

      if (!rsetBaseEvent.next()) {
        // Empty result set
        LOGGER.debug("No events found for channel: " + channel);
        return null;
      }

      // Populate event id, payload
      long eventId = rsetBaseEvent.getLong(1);
      peBuilder = new PersistableEvent.Builder(channel, eventId);
      peBuilder.setBasePayload(rsetBaseEvent.getBytes(2));
      boolean hasSpill = rsetBaseEvent.getBoolean(3);

      if (hasSpill) {
        spillEventFetchStmt =
            connection.prepareStatement(STMT_FETCH_PAYLOAD_SPILL);

        spillEventFetchStmt.setLong(1, eventId);
        ResultSet rsetSpillEvent = spillEventFetchStmt.executeQuery();
        if (!rsetSpillEvent.next()) {
          throw new JdbcChannelException("Payload spill expected but not "
              + "found for event: " + eventId);
        }
        Blob payloadSpillBlob = rsetSpillEvent.getBlob(1);
        payloadInputStream = payloadSpillBlob.getBinaryStream();
        ByteArrayOutputStream spillStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int length = 0;
        while ((length = payloadInputStream.read(buffer)) != -1) {
          spillStream.write(buffer, 0, length);
        }
        peBuilder.setSpillPayload(spillStream.toByteArray());

        // Delete this spill
        deleteSpillEventStmt =
            connection.prepareStatement(STMT_DELETE_EVENT_SPILL);
        deleteSpillEventStmt.setLong(1, eventId);

        int updateCount = deleteSpillEventStmt.executeUpdate();
        if (updateCount != 1) {
          throw new JdbcChannelException("Unexpected row count for spill "
              + "delete: " + updateCount);
        }
      }

      if (rsetBaseEvent.next()) {
        throw new JdbcChannelException("More than expected events retrieved");
      }

      // Populate headers
      List<Long> nameSpillHeaders = null;
      List<Long> valueSpillHeaders = null;
      baseHeaderFetchStmt = connection.prepareStatement(STMT_FETCH_HEADER_BASE);
      baseHeaderFetchStmt.setLong(1, eventId);
      int headerCount = 0; // for later delete validation

      ResultSet rsetBaseHeader = baseHeaderFetchStmt.executeQuery();
      while (rsetBaseHeader.next()) {
        headerCount++;
        long headerId = rsetBaseHeader.getLong(1);
        String baseName = rsetBaseHeader.getString(2);
        String baseValue = rsetBaseHeader.getString(3);
        boolean hasNameSpill = rsetBaseHeader.getBoolean(4);
        boolean hasValueSpill = rsetBaseHeader.getBoolean(5);

        peBuilder.setHeader(headerId, baseName, baseValue);
        if (hasNameSpill) {
          if (nameSpillHeaders == null) {
            nameSpillHeaders = new ArrayList<Long>();
          }
          nameSpillHeaders.add(headerId);
        }

        if (hasValueSpill) {
          if (valueSpillHeaders == null) {
            valueSpillHeaders = new ArrayList<Long>();
          }
          valueSpillHeaders.add(headerId);
        }
      }

      if (nameSpillHeaders != null) {

        nameSpillHeaderStmt =
            connection.prepareStatement(STMT_FETCH_HEADER_NAME_SPILL);

        deleteNameSpillHeaderStmt =
            connection.prepareStatement(STMT_DELETE_HEADER_NAME_SPILL);
        for (long headerId : nameSpillHeaders) {
          nameSpillHeaderStmt.setLong(1, headerId);
          ResultSet rsetHeaderNameSpill = nameSpillHeaderStmt.executeQuery();
          if (!rsetHeaderNameSpill.next()) {
            throw new JdbcChannelException("Name spill was set for header "
                + headerId + " but was not found");
          }
          String nameSpill = rsetHeaderNameSpill.getString(1);

          peBuilder.setHeaderNameSpill(headerId, nameSpill);
          deleteNameSpillHeaderStmt.setLong(1, headerId);
          deleteNameSpillHeaderStmt.addBatch();
        }

        // Delete header name spills
        int[] headerNameSpillDelete = deleteNameSpillHeaderStmt.executeBatch();
        if (headerNameSpillDelete.length != nameSpillHeaders.size()) {
          throw new JdbcChannelException("Unexpected number of header name "
              + "spill deletes: expected " + nameSpillHeaders.size()
              + ", found: " + headerNameSpillDelete.length);
        }

        for (int numRowsAffected : headerNameSpillDelete) {
          if (numRowsAffected != 1) {
            throw new JdbcChannelException("Unexpected number of deleted rows "
                + "for header name spill deletes: " + numRowsAffected);
          }
        }
      }

      if (valueSpillHeaders != null) {
        valueSpillHeaderStmt =
            connection.prepareStatement(STMT_FETCH_HEADER_VALUE_SPILL);

        deleteValueSpillHeaderStmt =
            connection.prepareStatement(STMT_DELETE_HEADER_VALUE_SPILL);
        for (long headerId: valueSpillHeaders) {
          valueSpillHeaderStmt.setLong(1, headerId);
          ResultSet rsetHeaderValueSpill = valueSpillHeaderStmt.executeQuery();
          if (!rsetHeaderValueSpill.next()) {
            throw new JdbcChannelException("Value spill was set for header "
                + headerId + " but was not found");
          }
          String valueSpill = rsetHeaderValueSpill.getString(1);

          peBuilder.setHeaderValueSpill(headerId, valueSpill);
          deleteValueSpillHeaderStmt.setLong(1, headerId);
          deleteValueSpillHeaderStmt.addBatch();
        }
        // Delete header value spills
        int[] headerValueSpillDelete = deleteValueSpillHeaderStmt.executeBatch();
        if (headerValueSpillDelete.length != valueSpillHeaders.size()) {
          throw new JdbcChannelException("Unexpected number of header value "
              + "spill deletes: expected " + valueSpillHeaders.size()
              + ", found: " + headerValueSpillDelete.length);
        }

        for (int numRowsAffected : headerValueSpillDelete) {
          if (numRowsAffected != 1) {
            throw new JdbcChannelException("Unexpected number of deleted rows "
                + "for header value spill deletes: " + numRowsAffected);
          }
        }
      }

      // Now delete Headers
      if (headerCount > 0) {
        deleteBaseHeaderStmt =
            connection.prepareStatement(STMT_DELETE_HEADER_BASE);
        deleteBaseHeaderStmt.setLong(1, eventId);

        int rowCount = deleteBaseHeaderStmt.executeUpdate();
        if (rowCount != headerCount) {
          throw new JdbcChannelException("Unexpected base header delete count: "
              + "expected: " + headerCount + ", found: " + rowCount);
        }
      }

      // Now delete the Event
      deleteBaseEventStmt = connection.prepareStatement(STMT_DELETE_EVENT_BASE);
      deleteBaseEventStmt.setLong(1, eventId);
      int rowCount = deleteBaseEventStmt.executeUpdate();

      if (rowCount != 1) {
        throw new JdbcChannelException("Unexpected row count for delete of "
            + "event-id: " + eventId + ", count: " + rowCount);
      }

    } catch (SQLException ex) {
      throw new JdbcChannelException("Unable to retrieve event", ex);
    } catch (IOException ex) {
      throw new JdbcChannelException("Unable to read data", ex);
    } finally {
      if (payloadInputStream != null) {
        try {
          payloadInputStream.close();
        } catch (IOException ex) {
          LOGGER.error("Unable to close payload spill stream", ex);
        }
      }
      if (baseEventFetchStmt != null) {
        try {
          baseEventFetchStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close base event fetch statement", ex);
        }
      }
      if (spillEventFetchStmt != null) {
        try {
          spillEventFetchStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close spill event fetch statment", ex);
        }
      }
      if (deleteSpillEventStmt != null) {
        try {
          deleteSpillEventStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close event spill delete statement", ex);
        }
      }
      if (baseHeaderFetchStmt != null) {
        try {
          baseHeaderFetchStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close base header fetch statement", ex);
        }
      }
      if (nameSpillHeaderStmt != null) {
        try {
          nameSpillHeaderStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close name spill fetch statement", ex);
        }
      }
      if (valueSpillHeaderStmt != null) {
        try {
          valueSpillHeaderStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close value spill fetch statement", ex);
        }
      }
      if (deleteNameSpillHeaderStmt != null) {
        try {
          deleteNameSpillHeaderStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close value spill delete statement", ex);
        }
      }
      if (deleteValueSpillHeaderStmt != null) {
        try {
          deleteValueSpillHeaderStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close value spill delete statement", ex);
        }
      }
      if (deleteBaseHeaderStmt != null) {
        try {
          deleteBaseHeaderStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close base header delete statement", ex);
        }
      }
      if (deleteBaseEventStmt != null) {
        try {
          deleteBaseEventStmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close base event delete statement", ex);
        }
      }
    }

    return peBuilder.build();
  }

  @Override
  public long getChannelSize(Connection connection) {
    long size = 0L;
    Statement stmt = null;
    try {
      stmt = connection.createStatement();
      stmt.execute(QUERY_CHANNEL_SIZE);
      ResultSet rset = stmt.getResultSet();
      if (!rset.next()) {
        throw new JdbcChannelException("Failed to determine channel size: "
              + "Query (" + QUERY_CHANNEL_SIZE
              + ") did not produce any results");
      }

      size = rset.getLong(1);
      connection.commit();
    } catch (SQLException ex) {
      try {
        connection.rollback();
      } catch (SQLException ex2) {
        LOGGER.error("Unable to rollback transaction", ex2);
      }
      throw new JdbcChannelException("Unable to run query: "
          + QUERY_CHANNEL_SIZE, ex);
    } finally {
      if (stmt != null) {
        try {
          stmt.close();
        } catch (SQLException ex) {
          LOGGER.error("Unable to close statement", ex);
        }
      }
    }

    return size;
  }
}
