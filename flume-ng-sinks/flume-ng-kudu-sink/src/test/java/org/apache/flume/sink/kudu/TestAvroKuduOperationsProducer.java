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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.junit.Rule;
import org.junit.Test;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.test.KuduTestHarness;

public class TestAvroKuduOperationsProducer {
  private static String schemaUriString;
  private static String schemaLiteral;

  static {
    try {
      String schemaPath = "/testAvroKuduOperationsProducer.avsc";
      URL schemaUrl = TestAvroKuduOperationsProducer.class.getResource(schemaPath);
      File schemaFile = Paths.get(schemaUrl.toURI()).toFile();
      schemaUriString = schemaFile.getAbsoluteFile().toURI().toString();
      schemaLiteral = Files.toString(schemaFile, UTF_8);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  enum SchemaLocation {
    GLOBAL, URL, LITERAL
  }

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Test
  public void testEmptyChannel() throws Exception {
    testEvents(0, SchemaLocation.GLOBAL);
  }

  @Test
  public void testOneEvent() throws Exception {
    testEvents(1, SchemaLocation.GLOBAL);
  }

  @Test
  public void testThreeEvents() throws Exception {
    testEvents(3, SchemaLocation.GLOBAL);
  }

  @Test
  public void testThreeEventsSchemaURLInEvent() throws Exception {
    testEvents(3, SchemaLocation.URL);
  }

  @Test
  public void testThreeEventsSchemaLiteralInEvent() throws Exception {
    testEvents(3, SchemaLocation.LITERAL);
  }

  private void testEvents(int eventCount, SchemaLocation schemaLocation)
      throws Exception {
    KuduTable table = createNewTable(
        String.format("test%sevents%s", eventCount, schemaLocation));
    String tableName = table.getName();
    Context context = schemaLocation != SchemaLocation.GLOBAL ? new Context()
        : new Context(ImmutableMap.of(KuduSinkConfigurationConstants.PRODUCER_PREFIX +
        AvroKuduOperationsProducer.SCHEMA_PROP, schemaUriString));
    context.put(KuduSinkConfigurationConstants.PRODUCER,
        AvroKuduOperationsProducer.class.getName());

    List<Event> events = generateEvents(eventCount, schemaLocation);

    KuduSinkTestUtil.processEventsCreatingSink(harness.getClient(), context, tableName, events);

    List<String> answers = makeAnswers(eventCount);
    List<String> rows = scanTableToStrings(table);
    assertEquals("wrong number of rows inserted", answers.size(), rows.size());
    assertArrayEquals("wrong rows inserted", answers.toArray(), rows.toArray());
  }

  private KuduTable createNewTable(String tableName) throws Exception {
    List<ColumnSchema> columns = new ArrayList<>(5);
    columns.add(new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("longField", Type.INT64).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("doubleField", Type.DOUBLE).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("nullableField", Type.STRING)
        .nullable(true).build());
    columns.add(new ColumnSchema.ColumnSchemaBuilder("stringField", Type.STRING).build());
    CreateTableOptions createOptions =
        new CreateTableOptions().setRangePartitionColumns(ImmutableList.of("key"))
            .setNumReplicas(1);
    return harness.getClient().createTable(tableName, new Schema(columns), createOptions);
  }

  private List<Event> generateEvents(int eventCount,
                                     SchemaLocation schemaLocation) throws Exception {
    List<Event> events = new ArrayList<>();
    for (int i = 0; i < eventCount; i++) {
      AvroKuduOperationsProducerTestRecord record = new AvroKuduOperationsProducerTestRecord();
      record.setKey(10 * i);
      record.setLongField(2L * i);
      record.setDoubleField(2.71828 * i);
      record.setNullableField(i % 2 == 0 ? null : "taco");
      record.setStringField(String.format("hello %d", i));
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      DatumWriter<AvroKuduOperationsProducerTestRecord> writer =
          new SpecificDatumWriter<>(AvroKuduOperationsProducerTestRecord.class);
      writer.write(record, encoder);
      encoder.flush();
      Event e = EventBuilder.withBody(out.toByteArray());
      if (schemaLocation == SchemaLocation.URL) {
        e.setHeaders(ImmutableMap.of(AvroKuduOperationsProducer.SCHEMA_URL_HEADER,
            schemaUriString));
      } else if (schemaLocation == SchemaLocation.LITERAL) {
        e.setHeaders(ImmutableMap.of(AvroKuduOperationsProducer.SCHEMA_LITERAL_HEADER,
            schemaLiteral));
      }
      events.add(e);
    }
    return events;
  }

  private List<String> makeAnswers(int eventCount) {
    List<String> answers = Lists.newArrayList();
    for (int i = 0; i < eventCount; i++) {
      answers.add(String.format(
          "INT32 key=%s, INT64 longField=%s, DOUBLE doubleField=%s, " +
              "STRING nullableField=%s, STRING stringField=hello %s",
          10 * i,
          2 * i,
          2.71828 * i,
          i % 2 == 0 ? "NULL" : "taco",
          i));
    }
    Collections.sort(answers);
    return answers;
  }
}
