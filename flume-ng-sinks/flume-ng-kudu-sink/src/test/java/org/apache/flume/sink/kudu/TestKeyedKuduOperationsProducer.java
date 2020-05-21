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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kudu.test.ClientTestUtil.scanTableToStrings;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.test.KuduTestHarness;

public class TestKeyedKuduOperationsProducer {
  private static final Logger LOG = LoggerFactory.getLogger(TestKeyedKuduOperationsProducer.class);

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  private KuduTable createNewTable(String tableName) throws Exception {
    LOG.info("Creating new table...");

    ArrayList<ColumnSchema> columns = new ArrayList<>(2);
    columns.add(
        new ColumnSchema.ColumnSchemaBuilder(
            SimpleKeyedKuduOperationsProducer.KEY_COLUMN_DEFAULT, Type.STRING)
            .key(true).build());
    columns.add(
        new ColumnSchema.ColumnSchemaBuilder(
            SimpleKeyedKuduOperationsProducer.PAYLOAD_COLUMN_DEFAULT, Type.BINARY)
            .key(false).build());
    CreateTableOptions createOptions =
        new CreateTableOptions()
            .setRangePartitionColumns(ImmutableList.of(
                SimpleKeyedKuduOperationsProducer.KEY_COLUMN_DEFAULT))
            .setNumReplicas(1);
    KuduTable table =
        harness.getClient().createTable(tableName, new Schema(columns), createOptions);

    LOG.info("Created new table.");

    return table;
  }

  @Test
  public void testEmptyChannelWithInsert() throws Exception {
    testEvents(0, "insert");
  }

  @Test
  public void testOneEventWithInsert() throws Exception {
    testEvents(1, "insert");
  }

  @Test
  public void testThreeEventsWithInsert() throws Exception {
    testEvents(3, "insert");
  }

  @Test
  public void testEmptyChannelWithUpsert() throws Exception {
    testEvents(0, "upsert");
  }

  @Test
  public void testOneEventWithUpsert() throws Exception {
    testEvents(1, "upsert");
  }

  @Test
  public void testThreeEventsWithUpsert() throws Exception {
    testEvents(3, "upsert");
  }

  @Test
  public void testDuplicateRowsWithUpsert() throws Exception {
    LOG.info("Testing events with upsert...");

    KuduTable table = createNewTable("testDupUpsertEvents");
    String tableName = table.getName();
    Context ctx = new Context(ImmutableMap.of(
        KuduSinkConfigurationConstants.PRODUCER_PREFIX +
            SimpleKeyedKuduOperationsProducer.OPERATION_PROP, "upsert",
        KuduSinkConfigurationConstants.PRODUCER, SimpleKeyedKuduOperationsProducer.class.getName()
    ));
    KuduSink sink = KuduSinkTestUtil.createSink(harness.getClient(), tableName, ctx);
    sink.start();

    int numRows = 3;
    List<Event> events = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      Event e = EventBuilder.withBody(String.format("payload body %s", i), UTF_8);
      e.setHeaders(ImmutableMap.of(SimpleKeyedKuduOperationsProducer.KEY_COLUMN_DEFAULT,
          String.format("key %s", i)));
      events.add(e);
    }

    KuduSinkTestUtil.processEvents(sink, events);

    List<String> rows = scanTableToStrings(table);
    assertEquals(numRows + " row(s) expected", numRows, rows.size());

    for (int i = 0; i < numRows; i++) {
      assertTrue("incorrect payload", rows.get(i).contains("payload body " + i));
    }

    Event dup = EventBuilder.withBody("payload body upserted".getBytes(UTF_8));
    dup.setHeaders(ImmutableMap.of("key", String.format("key %s", 0)));

    KuduSinkTestUtil.processEvents(sink, ImmutableList.of(dup));

    List<String> upRows = scanTableToStrings(table);
    assertEquals(numRows + " row(s) expected", numRows, upRows.size());

    assertTrue("incorrect payload", upRows.get(0).contains("payload body upserted"));
    for (int i = 1; i < numRows; i++) {
      assertTrue("incorrect payload", upRows.get(i).contains("payload body " + i));
    }

    LOG.info("Testing events with upsert finished successfully.");
  }

  private void testEvents(int eventCount, String operation) throws Exception {
    LOG.info("Testing {} events...", eventCount);

    KuduTable table = createNewTable("test" + eventCount + "events" + operation);
    String tableName = table.getName();
    Context context = new Context(ImmutableMap.of(
        KuduSinkConfigurationConstants.PRODUCER_PREFIX +
            SimpleKeyedKuduOperationsProducer.OPERATION_PROP, operation,
        KuduSinkConfigurationConstants.PRODUCER, SimpleKeyedKuduOperationsProducer.class.getName()
    ));

    List<Event> events = getEvents(eventCount);

    KuduSinkTestUtil.processEventsCreatingSink(harness.getClient(), context, tableName, events);

    List<String> rows = scanTableToStrings(table);
    assertEquals(eventCount + " row(s) expected", eventCount, rows.size());

    for (int i = 0; i < eventCount; i++) {
      assertTrue("incorrect payload", rows.get(i).contains("payload body " + i));
    }

    LOG.info("Testing {} events finished successfully.", eventCount);
  }

  private List<Event> getEvents(int eventCount) {
    List<Event> events = new ArrayList<>();
    for (int i = 0; i < eventCount; i++) {
      Event e = EventBuilder.withBody(String.format("payload body %s", i).getBytes(UTF_8));
      e.setHeaders(ImmutableMap.of("key", String.format("key %s", i)));
      events.add(e);
    }
    return events;
  }
}
