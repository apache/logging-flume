/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.sink.kudu;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;

import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.Upsert;

/**
 * A simple serializer that generates one {@link Insert} or {@link Upsert}
 * per {@link Event} by writing the event body into a BINARY column. The pair
 * (key column name, key column value) should be a header in the {@link Event};
 * the column name is configurable but the column type must be STRING. Multiple
 * key columns are not supported.
 *
 * <p><strong>Simple Keyed Kudu Operations Producer configuration parameters</strong>
 *
 * <table cellpadding=3 cellspacing=0 border=1
 *        summary="Simple Keyed Kudu Operations Producer configuration parameters">
 * <tr>
 *   <th>Property Name</th>
 *   <th>Default</th>
 *   <th>Required?</th>
 *   <th>Description</th>
 * </tr>
 * <tr>
 *   <td>producer.payloadColumn</td>
 *   <td>payload</td>
 *   <td>No</td>
 *   <td>The name of the BINARY column to write the Flume event body to.</td>
 * </tr>
 * <tr>
 *   <td>producer.keyColumn</td>
 *   <td>key</td>
 *   <td>No</td>
 *   <td>The name of the STRING key column of the target Kudu table.</td>
 * </tr>
 * <tr>
 *   <td>producer.operation</td>
 *   <td>upsert</td>
 *   <td>No</td>
 *   <td>The operation used to write events to Kudu. Supported operations
 *   are 'insert' and 'upsert'</td>
 * </tr>
 * </table>
 */
public class SimpleKeyedKuduOperationsProducer implements KuduOperationsProducer {
  public static final String PAYLOAD_COLUMN_PROP = "payloadColumn";
  public static final String PAYLOAD_COLUMN_DEFAULT = "payload";
  public static final String KEY_COLUMN_PROP = "keyColumn";
  public static final String KEY_COLUMN_DEFAULT = "key";
  public static final String OPERATION_PROP = "operation";
  public static final String OPERATION_DEFAULT = "upsert";

  private KuduTable table;
  private String payloadColumn;
  private String keyColumn;
  private String operation = "";

  public SimpleKeyedKuduOperationsProducer(){
  }

  @Override
  public void configure(Context context) {
    payloadColumn = context.getString(PAYLOAD_COLUMN_PROP, PAYLOAD_COLUMN_DEFAULT);
    keyColumn = context.getString(KEY_COLUMN_PROP, KEY_COLUMN_DEFAULT);
    operation = context.getString(OPERATION_PROP, OPERATION_DEFAULT);
  }

  @Override
  public void initialize(KuduTable table) {
    this.table = table;
  }

  @Override
  public List<Operation> getOperations(Event event) throws FlumeException {
    String key = event.getHeaders().get(keyColumn);
    if (key == null) {
      throw new FlumeException(
          String.format("No value provided for key column %s", keyColumn));
    }
    try {
      Operation op;
      switch (operation.toLowerCase(Locale.ENGLISH)) {
        case "upsert":
          op = table.newUpsert();
          break;
        case "insert":
          op = table.newInsert();
          break;
        default:
          throw new FlumeException(
              String.format("Unexpected operation %s", operation));
      }
      PartialRow row = op.getRow();
      row.addString(keyColumn, key);
      row.addBinary(payloadColumn, event.getBody());

      return Collections.singletonList(op);
    } catch (Exception e) {
      throw new FlumeException("Failed to create Kudu Operation object", e);
    }
  }

  @Override
  public void close() {
  }
}

