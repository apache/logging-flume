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

import org.apache.flume.channel.jdbc.impl.DerbySchemaHandler;
import org.junit.Assert;
import org.junit.Test;

public class TestDerbySchemaHandlerQueries {

  public static final String EXPECTED_QUERY_CREATE_SCHEMA_FLUME
       = "CREATE SCHEMA FLUME";

  public static final String EXPECTED_QUERY_CREATE_TABLE_FL_EVENT
       = "CREATE TABLE FLUME.FL_EVENT ( FLE_ID BIGINT GENERATED ALWAYS AS "
           + "IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, FLE_PAYLOAD "
           + "VARCHAR(16384) FOR BIT DATA, FLE_CHANNEL VARCHAR(64), "
           + "FLE_SPILL BOOLEAN)";

  public static final String EXPECTED_QUERY_CREATE_INDEX_FLE_CHANNEL
       = "CREATE INDEX FLUME.IDX_FLE_CHANNEL ON FLUME.FL_EVENT (FLE_CHANNEL)";

  public static final String EXPECTED_QUERY_CREATE_TABLE_FL_PLSPILL_FK
       = "CREATE TABLE FLUME.FL_PLSPILL ( FLP_EVENT BIGINT, FLP_SPILL BLOB, "
           + "FOREIGN KEY (FLP_EVENT) REFERENCES FLUME.FL_EVENT (FLE_ID))";

  public static final String EXPECTED_QUERY_CREATE_TABLE_FL_PLSPILL_NOFK
      = "CREATE TABLE FLUME.FL_PLSPILL ( FLP_EVENT BIGINT, FLP_SPILL BLOB)";

  public static final String EXPECTED_QUERY_CREATE_INDEX_FLP_EVENT
       = "CREATE INDEX FLUME.IDX_FLP_EVENT ON FLUME.FL_PLSPILL (FLP_EVENT)";

  public static final String EXPECTED_QUERY_CREATE_TABLE_FL_HEADER_FK
       = "CREATE TABLE FLUME.FL_HEADER ( FLH_ID BIGINT GENERATED ALWAYS AS "
           + "IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
           + "FLH_EVENT BIGINT, FLH_NAME VARCHAR(251), "
           + "FLH_VALUE VARCHAR(251), FLH_NMSPILL BOOLEAN, "
           + "FLH_VLSPILL BOOLEAN, FOREIGN KEY (FLH_EVENT) "
           + "REFERENCES FLUME.FL_EVENT (FLE_ID))";

  public static final String EXPECTED_QUERY_CREATE_TABLE_FL_HEADER_NOFK
      = "CREATE TABLE FLUME.FL_HEADER ( FLH_ID BIGINT GENERATED ALWAYS AS "
           + "IDENTITY (START WITH 1, INCREMENT BY 1) PRIMARY KEY, "
           + "FLH_EVENT BIGINT, FLH_NAME VARCHAR(251), "
           + "FLH_VALUE VARCHAR(251), FLH_NMSPILL BOOLEAN, "
           + "FLH_VLSPILL BOOLEAN)";

  public static final String EXPECTED_QUERY_CREATE_INDEX_FLH_EVENT
       = "CREATE INDEX FLUME.IDX_FLH_EVENT ON FLUME.FL_HEADER (FLH_EVENT)";

  public static final String EXPECTED_QUERY_CREATE_TABLE_FL_NMSPILL_FK
       = "CREATE TABLE FLUME.FL_NMSPILL ( FLN_HEADER BIGINT, FLN_SPILL "
           + "VARCHAR(32517), FOREIGN KEY (FLN_HEADER) REFERENCES "
           + "FLUME.FL_HEADER (FLH_ID))";

  public static final String EXPECTED_QUERY_CREATE_TABLE_FL_NMSPILL_NOFK
      = "CREATE TABLE FLUME.FL_NMSPILL ( FLN_HEADER BIGINT, FLN_SPILL "
           + "VARCHAR(32517))";

  public static final String EXPECTED_QUERY_CREATE_INDEX_FLN_HEADER
       = "CREATE INDEX FLUME.IDX_FLN_HEADER ON FLUME.FL_NMSPILL (FLN_HEADER)";

  public static final String EXPECTED_QUERY_CREATE_TABLE_FL_VLSPILL_FK
       = "CREATE TABLE FLUME.FL_VLSPILL ( FLV_HEADER BIGINT, FLV_SPILL "
            + "VARCHAR(32517), FOREIGN KEY (FLV_HEADER) REFERENCES "
            + "FLUME.FL_HEADER (FLH_ID))";

  public static final String EXPECTED_QUERY_CREATE_TABLE_FL_VLSPILL_NOFK
      = "CREATE TABLE FLUME.FL_VLSPILL ( FLV_HEADER BIGINT, FLV_SPILL "
            + "VARCHAR(32517))";

  public static final String EXPECTED_QUERY_CREATE_INDEX_FLV_HEADER
       = "CREATE INDEX FLUME.IDX_FLV_HEADER ON FLUME.FL_VLSPILL (FLV_HEADER)";

  public static final String EXPECTED_COLUMN_LOOKUP_QUERY
      = "SELECT COLUMNNAME from SYS.SYSCOLUMNS where REFERENCEID = "
          + "(SELECT TABLEID FROM SYS.SYSTABLES WHERE TABLENAME = ? AND "
          + "SCHEMAID = (SELECT SCHEMAID FROM SYS.SYSSCHEMAS WHERE "
          + "SCHEMANAME = ? ))";

  public static final String EXPECTED_QUERY_CHANNEL_SIZE
      = "SELECT COUNT(*) FROM FLUME.FL_EVENT";

  public static final String EXPECTED_STMT_INSERT_EVENT_BASE
      = "INSERT INTO FLUME.FL_EVENT (FLE_PAYLOAD, FLE_CHANNEL, FLE_SPILL) "
          + "VALUES ( ?, ?, ?)";

  public static final String EXPECTED_STMT_INSERT_EVENT_SPILL
      = "INSERT INTO FLUME.FL_PLSPILL (FLP_EVENT, FLP_SPILL) VALUES ( ?, ?)";

  public static final String EXPECTED_STMT_INSERT_HEADER_BASE
      = "INSERT INTO FLUME.FL_HEADER (FLH_EVENT, FLH_NAME, FLH_VALUE, "
          + "FLH_NMSPILL, FLH_VLSPILL) VALUES ( ?, ?, ?, ?, ?)";


  public static final String EXPECTED_STMT_INSERT_HEADER_NAME_SPILL
      = "INSERT INTO FLUME.FL_NMSPILL (FLN_HEADER, FLN_SPILL) VALUES ( ?, ?)";

  public static final String EXPECTED_STMT_INSERT_HEADER_VALUE_SPILL
      = "INSERT INTO FLUME.FL_VLSPILL (FLV_HEADER, FLV_SPILL) VALUES ( ?, ?)";

  public static final String EXPECTED_STMT_FETCH_PAYLOAD_BASE
      = "SELECT FLE_ID, FLE_PAYLOAD, FLE_SPILL FROM FLUME.FL_EVENT WHERE "
          + "FLE_ID = (SELECT MIN(FLE_ID) FROM FLUME.FL_EVENT WHERE "
          + "FLE_CHANNEL = ?)";


  public static final String EXPECTED_STMT_FETCH_PAYLOAD_SPILL
      = "SELECT FLP_SPILL FROM FLUME.FL_PLSPILL WHERE FLP_EVENT = ?";

  public static final String EXPECTED_STMT_FETCH_HEADER_BASE
      = "SELECT FLH_ID, FLH_NAME, FLH_VALUE, FLH_NMSPILL, FLH_VLSPILL FROM "
          + "FLUME.FL_HEADER WHERE FLH_EVENT = ?";

  public static final String EXPECTED_STMT_FETCH_HEADER_NAME_SPILL
      = "SELECT FLN_SPILL FROM FLUME.FL_NMSPILL WHERE FLN_HEADER = ?";

  public static final String EXPECTED_STMT_FETCH_HEADER_VALUE_SPILL
      = "SELECT FLV_SPILL FROM FLUME.FL_VLSPILL WHERE FLV_HEADER = ?";

  public static final String EXPECTED_STMT_DELETE_HEADER_VALUE_SPILL
      = "DELETE FROM FLUME.FL_VLSPILL WHERE FLV_HEADER = ?";

  public static final String EXPECTED_STMT_DELETE_HEADER_NAME_SPILL
      = "DELETE FROM FLUME.FL_NMSPILL WHERE FLN_HEADER = ?";

  public static final String EXPECTED_STMT_DELETE_EVENT_SPILL
      = "DELETE FROM FLUME.FL_PLSPILL WHERE FLP_EVENT = ?";

  public static final String EXPECTED_STMT_DELETE_HEADER_BASE
      = "DELETE FROM FLUME.FL_HEADER WHERE FLH_EVENT = ?";

  public static final String EXPECTED_STMT_DELETE_EVENT_BASE
      = "DELETE FROM FLUME.FL_EVENT WHERE FLE_ID = ?";


  @Test
  public void testCreateQueries() {

    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_SCHEMA_FLUME,
        EXPECTED_QUERY_CREATE_SCHEMA_FLUME);

    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_TABLE_FL_EVENT,
        EXPECTED_QUERY_CREATE_TABLE_FL_EVENT);

    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_INDEX_FLE_CHANNEL,
        EXPECTED_QUERY_CREATE_INDEX_FLE_CHANNEL);

    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_TABLE_FL_PLSPILL_FK,
        EXPECTED_QUERY_CREATE_TABLE_FL_PLSPILL_FK);

    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_TABLE_FL_PLSPILL_NOFK,
        EXPECTED_QUERY_CREATE_TABLE_FL_PLSPILL_NOFK);


    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_INDEX_FLP_EVENT,
        EXPECTED_QUERY_CREATE_INDEX_FLP_EVENT);

    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_TABLE_FL_HEADER_FK,
        EXPECTED_QUERY_CREATE_TABLE_FL_HEADER_FK);

    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_TABLE_FL_HEADER_NOFK,
        EXPECTED_QUERY_CREATE_TABLE_FL_HEADER_NOFK);

    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_INDEX_FLH_EVENT,
        EXPECTED_QUERY_CREATE_INDEX_FLH_EVENT);

    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_TABLE_FL_NMSPILL_FK,
        EXPECTED_QUERY_CREATE_TABLE_FL_NMSPILL_FK);

    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_TABLE_FL_NMSPILL_NOFK,
        EXPECTED_QUERY_CREATE_TABLE_FL_NMSPILL_NOFK);

    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_INDEX_FLN_HEADER,
        EXPECTED_QUERY_CREATE_INDEX_FLN_HEADER);

    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_TABLE_FL_VLSPILL_FK,
        EXPECTED_QUERY_CREATE_TABLE_FL_VLSPILL_FK);

    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_TABLE_FL_VLSPILL_NOFK,
        EXPECTED_QUERY_CREATE_TABLE_FL_VLSPILL_NOFK);

    Assert.assertEquals(DerbySchemaHandler.QUERY_CREATE_INDEX_FLV_HEADER,
        EXPECTED_QUERY_CREATE_INDEX_FLV_HEADER);

    Assert.assertEquals(DerbySchemaHandler.COLUMN_LOOKUP_QUERY,
        EXPECTED_COLUMN_LOOKUP_QUERY);

    Assert.assertEquals(DerbySchemaHandler.QUERY_CHANNEL_SIZE,
        EXPECTED_QUERY_CHANNEL_SIZE);

    Assert.assertEquals(DerbySchemaHandler.STMT_INSERT_EVENT_BASE,
        EXPECTED_STMT_INSERT_EVENT_BASE);

    Assert.assertEquals(DerbySchemaHandler.STMT_INSERT_EVENT_SPILL,
        EXPECTED_STMT_INSERT_EVENT_SPILL);

    Assert.assertEquals(DerbySchemaHandler.STMT_INSERT_HEADER_BASE,
        EXPECTED_STMT_INSERT_HEADER_BASE);

    Assert.assertEquals(DerbySchemaHandler.STMT_INSERT_HEADER_NAME_SPILL,
        EXPECTED_STMT_INSERT_HEADER_NAME_SPILL);

    Assert.assertEquals(DerbySchemaHandler.STMT_INSERT_HEADER_VALUE_SPILL,
        EXPECTED_STMT_INSERT_HEADER_VALUE_SPILL);

    Assert.assertEquals(DerbySchemaHandler.STMT_FETCH_PAYLOAD_BASE,
        EXPECTED_STMT_FETCH_PAYLOAD_BASE);

    Assert.assertEquals(DerbySchemaHandler.STMT_FETCH_PAYLOAD_SPILL,
        EXPECTED_STMT_FETCH_PAYLOAD_SPILL);

    Assert.assertEquals(DerbySchemaHandler.STMT_FETCH_HEADER_BASE,
        EXPECTED_STMT_FETCH_HEADER_BASE);

    Assert.assertEquals(DerbySchemaHandler.STMT_FETCH_HEADER_NAME_SPILL,
        EXPECTED_STMT_FETCH_HEADER_NAME_SPILL);

    Assert.assertEquals(DerbySchemaHandler.STMT_FETCH_HEADER_VALUE_SPILL,
        EXPECTED_STMT_FETCH_HEADER_VALUE_SPILL);

    Assert.assertEquals(DerbySchemaHandler.STMT_DELETE_HEADER_VALUE_SPILL,
        EXPECTED_STMT_DELETE_HEADER_VALUE_SPILL);

    Assert.assertEquals(DerbySchemaHandler.STMT_DELETE_HEADER_NAME_SPILL,
        EXPECTED_STMT_DELETE_HEADER_NAME_SPILL);

    Assert.assertEquals(DerbySchemaHandler.STMT_DELETE_EVENT_SPILL,
        EXPECTED_STMT_DELETE_EVENT_SPILL);

    Assert.assertEquals(DerbySchemaHandler.STMT_DELETE_HEADER_BASE,
        EXPECTED_STMT_DELETE_HEADER_BASE);

    Assert.assertEquals(DerbySchemaHandler.STMT_DELETE_EVENT_BASE,
        EXPECTED_STMT_DELETE_EVENT_BASE);

  }

}
