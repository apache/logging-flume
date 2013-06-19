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
package org.apache.flume.serialization;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.codec.binary.Hex;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;

public class TestAvroEventDeserializer {

  private static final Logger logger =
      LoggerFactory.getLogger(TestAvroEventDeserializer.class);

  private static final Schema schema;
  static {
    schema = Schema.createRecord("MyRecord", "", "org.apache.flume",  false);
    Schema.Field field = new Schema.Field("foo",
        Schema.create(Schema.Type.STRING), "", null);
    schema.setFields(Collections.singletonList(field));
  }

  @Test
  public void resetTest() throws IOException {
    File tempFile = newTestFile(true);

    String target = tempFile.getAbsolutePath();
    logger.info("Target: {}", target);
    TransientPositionTracker tracker = new TransientPositionTracker(target);

    AvroEventDeserializer.Builder desBuilder =
        new AvroEventDeserializer.Builder();
    EventDeserializer deserializer = desBuilder.build(new Context(),
        new ResettableFileInputStream(tempFile, tracker));

    BinaryDecoder decoder = null;
    DatumReader<GenericRecord> reader =
        new GenericDatumReader<GenericRecord>(schema);

    decoder = DecoderFactory.get().binaryDecoder(
        deserializer.readEvent().getBody(), decoder);
    assertEquals("bar", reader.read(null, decoder).get("foo").toString());

    deserializer.reset();

    decoder = DecoderFactory.get().binaryDecoder(
        deserializer.readEvent().getBody(), decoder);
    assertEquals("bar", reader.read(null, decoder).get("foo").toString());

    deserializer.mark();

    decoder = DecoderFactory.get().binaryDecoder(
        deserializer.readEvent().getBody(), decoder);
    assertEquals("baz", reader.read(null, decoder).get("foo").toString());

    deserializer.reset();

    decoder = DecoderFactory.get().binaryDecoder(
        deserializer.readEvent().getBody(), decoder);
    assertEquals("baz", reader.read(null, decoder).get("foo").toString());

    assertNull(deserializer.readEvent());
  }

  @Test
  public void testSchemaHash() throws IOException, NoSuchAlgorithmException {
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
        new AvroEventDeserializer.Builder().build(context, in);

    Event event = des.readEvent();
    String eventSchemaHash =
        event.getHeaders().get(AvroEventDeserializer.AVRO_SCHEMA_HEADER_HASH);
    String expectedSchemaHash = Hex.encodeHexString(
        SchemaNormalization.parsingFingerprint("CRC-64-AVRO", schema));

    Assert.assertEquals(expectedSchemaHash, eventSchemaHash);
  }

  @Test
  public void testSchemaLiteral() throws IOException {
    File tempFile = newTestFile(true);

    String target = tempFile.getAbsolutePath();
    logger.info("Target: {}", target);
    TransientPositionTracker tracker = new TransientPositionTracker(target);

    Context context = new Context();
    context.put(AvroEventDeserializer.CONFIG_SCHEMA_TYPE_KEY,
        AvroEventDeserializer.AvroSchemaType.LITERAL.toString());

    ResettableInputStream in =
        new ResettableFileInputStream(tempFile, tracker);
    EventDeserializer des =
        new AvroEventDeserializer.Builder().build(context, in);

    Event event = des.readEvent();
    String eventSchema =
        event.getHeaders().get(AvroEventDeserializer.AVRO_SCHEMA_HEADER_LITERAL);

    Assert.assertEquals(schema.toString(), eventSchema);
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
    recordBuilder.set("foo", "bar");
    GenericRecord record = recordBuilder.build();
    writer.append(record);
    writer.sync();
    recordBuilder = new GenericRecordBuilder(schema);
    recordBuilder.set("foo", "baz");
    record = recordBuilder.build();
    writer.append(record);
    writer.sync();
    writer.flush();
    writer.close();

    return tempFile;
  }
}
