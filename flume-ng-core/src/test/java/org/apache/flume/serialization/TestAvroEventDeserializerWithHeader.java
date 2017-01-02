/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.serialization;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.codec.binary.Hex;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestAvroEventDeserializerWithHeader {

  private static final Logger logger =
          LoggerFactory.getLogger(TestAvroEventDeserializerWithHeader.class);

  private static final Schema schema;

  static {
    schema = Schema.createRecord("MyRecord", "", "org.apache.flume", false);
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
    Schema.Field headers = new Schema.Field("headers", mapSchema, "", null);
    Schema.Field body = new Schema.Field("body", Schema.create(Schema.Type.BYTES), "", null);
    List<Schema.Field> fields = new ArrayList<Schema.Field>();
    fields.add(headers);
    fields.add(body);
    schema.setFields(fields);
  }

  @Test
  public void testWithHeaderAndFooter() throws IOException, NoSuchAlgorithmException {
    File tempFile = newTestFile(true);

    String target = tempFile.getAbsolutePath();
    logger.info("Target: {}", target);
    TransientPositionTracker tracker = new TransientPositionTracker(target);

    Context context = new Context();
    context.put(AvroEventDeserializer.CONFIG_SCHEMA_TYPE_KEY,
            AvroEventDeserializer.AvroSchemaType.HASH.toString());

    ResettableInputStream in =
            new ResettableFileInputStream(tempFile, tracker);
    EventDeserializer des =
            new FlumeEventAvroEventDeserializer.Builder().build(context, in);

    Event event = des.readEvent();
    String eventSchemaHash =
            event.getHeaders().get(AvroEventDeserializer.AVRO_SCHEMA_HEADER_HASH);
    String expectedSchemaHash = Hex.encodeHexString(
            SchemaNormalization.parsingFingerprint("CRC-64-AVRO", schema));

    Map<String, String> headers = event.getHeaders();

    Assert.assertTrue(headers.containsKey("key-a"));
    Assert.assertTrue(headers.containsValue("value-b"));

    Assert.assertEquals(expectedSchemaHash, eventSchemaHash);
  }

  private File newTestFile(boolean deleteOnExit) throws IOException {
    File tempFile = File.createTempFile("testDirectFile", "tmp");
    if (deleteOnExit) {
      tempFile.deleteOnExit();
    }

    DataFileWriter<GenericRecord> writer =
            new DataFileWriter<GenericRecord>(
                    new GenericDatumWriter<GenericRecord>(schema));
    writer.create(schema, tempFile);

    GenericRecordBuilder recordBuilder;
    recordBuilder = new GenericRecordBuilder(schema);

    Map<String, String> headers = new HashMap<>();
    headers.put("key-a", "value-b");
    recordBuilder.set("headers", headers);

    recordBuilder.set("body", ByteBuffer.wrap("body".getBytes()));

    GenericRecord record = recordBuilder.build();
    writer.append(record);
    writer.sync();

    writer.flush();
    writer.close();

    return tempFile;
  }
}
