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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduTable;

public class TestRegexpKuduOperationsProducer {
  private static final String TEST_REGEXP =
      "(?<key>\\d+),(?<byteFld>\\d+),(?<shortFld>\\d+),(?<intFld>\\d+)," +
      "(?<longFld>\\d+),(?<binaryFld>\\w+),(?<stringFld>\\w+),(?<boolFld>\\w+)," +
      "(?<floatFld>\\d+\\.\\d*),(?<doubleFld>\\d+.\\d*)";

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  private KuduTable createNewTable(String tableName) throws Exception {
    ArrayList<ColumnSchema> columns = new ArrayList<>(10);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("byteFld", Type.INT8).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("shortFld", Type.INT16).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("intFld", Type.INT32).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("longFld", Type.INT64).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("binaryFld", Type.BINARY).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("stringFld", Type.STRING).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("boolFld", Type.BOOL).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("floatFld", Type.FLOAT).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("doubleFld", Type.DOUBLE).build());
    CreateTableOptions createOptions =
        new CreateTableOptions().addHashPartitions(ImmutableList.of("key"), 3).setNumReplicas(1);
    return harness.getClient().createTable(tableName, new Schema(columns), createOptions);
  }

  @Test
  public void testEmptyChannel() throws Exception {
    testEvents(0, 1, "insert");
  }

  @Test
  public void testOneEvent() throws Exception {
    testEvents(1, 1, "insert");
  }

  @Test
  public void testThreeEvents() throws Exception {
    testEvents(3, 1, "insert");
  }

  @Test
  public void testThreeEventsWithUpsert() throws Exception {
    testEvents(3, 1, "upsert");
  }

  @Test
  public void testOneEventTwoRowsEach() throws Exception {
    testEvents(1, 2, "insert");
  }

  @Test
  public void testTwoEventsTwoRowsEach() throws Exception {
    testEvents(2, 2, "insert");
  }

  @Test
  public void testTwoEventsTwoRowsEachWithUpsert() throws Exception {
    testEvents(2, 2, "upsert");
  }

  private void testEvents(int eventCount, int perEventRowCount, String operation) throws Exception {
    String tableName = String.format("test%sevents%srowseach%s",
        eventCount, perEventRowCount, operation);
    Context context = new Context();
    context.put(KuduSinkConfigurationConstants.PRODUCER_PREFIX +
        RegexpKuduOperationsProducer.PATTERN_PROP, TEST_REGEXP);
    context.put(KuduSinkConfigurationConstants.PRODUCER_PREFIX +
        RegexpKuduOperationsProducer.OPERATION_PROP, operation);
    context.put(KuduSinkConfigurationConstants.PRODUCER,
        RegexpKuduOperationsProducer.class.getName());
    KuduTable table = createNewTable(tableName);

    List<Event> events = generateEvents(eventCount, perEventRowCount, operation);

    KuduSinkTestUtil.processEventsCreatingSink(harness.getClient(), context, tableName, events);

    List<String> rows = scanTableToStrings(table);
    assertEquals(eventCount * perEventRowCount + " row(s) expected",
        eventCount * perEventRowCount,
        rows.size());

    ArrayList<String> rightAnswers = new ArrayList<>(eventCount * perEventRowCount);
    for (int i = 0; i < eventCount; i++) {
      for (int j = 0; j < perEventRowCount; j++) {
        int value = operation.equals("upsert") && i == 0 ? 1 : i;
        String baseAnswer = "INT32 key=1%2$d%3$d1, INT8 byteFld=%1$d, INT16 shortFld=%1$d, " +
            "INT32 intFld=%1$d, INT64 longFld=%1$d, BINARY binaryFld=\"binary\", " +
            "STRING stringFld=string, BOOL boolFld=false, FLOAT floatFld=%1$d.%1$d, " +
            "DOUBLE doubleFld=%1$d.%1$d";
        String rightAnswer = String.format(baseAnswer, value, i, j);
        rightAnswers.add(rightAnswer);
      }
    }
    Collections.sort(rightAnswers);

    for (int k = 0; k < eventCount * perEventRowCount; k++) {
      assertEquals("incorrect row", rightAnswers.get(k), rows.get(k));
    }
  }

  private List<Event> generateEvents(int eventCount, int perEventRowCount, String operation) {
    List<Event> events = new ArrayList<>();

    for (int i = 0; i < eventCount; i++) {
      StringBuilder payload = new StringBuilder();
      for (int j = 0; j < perEventRowCount; j++) {
        String baseRow = "|1%1$d%2$d1,%1$d,%1$d,%1$d,%1$d,binary," +
            "string,false,%1$d.%1$d,%1$d.%1$d,%1$d.%1$d|";
        String row = String.format(baseRow, i, j);
        payload.append(row);
      }
      Event e = EventBuilder.withBody(payload.toString().getBytes(UTF_8));
      events.add(e);
    }

    if (eventCount > 0) {
      // In the upsert case, add one upsert row per insert event (i.e. per i)
      // All such rows go in one event.
      if (operation.equals("upsert")) {
        StringBuilder upserts = new StringBuilder();
        for (int j = 0; j < perEventRowCount; j++) {
          String row = String.format("|1%2$d%3$d1,%1$d,%1$d,%1$d,%1$d,binary," +
              "string,false,%1$d.%1$d,%1$d.%1$d,%1$d.%1$d|", 1, 0, j);
          upserts.append(row);
        }
        Event e = EventBuilder.withBody(upserts.toString().getBytes(UTF_8));
        events.add(e);
      }

      // Also check some bad/corner cases.
      String mismatchInInt = "|1,2,taco,4,5,x,y,true,1.0.2.0,999|";
      String emptyString = "";
      String[] testCases = {mismatchInInt, emptyString};
      for (String testCase : testCases) {
        Event e = EventBuilder.withBody(testCase.getBytes(UTF_8));
        events.add(e);
      }
    }
    return events;
  }
}
