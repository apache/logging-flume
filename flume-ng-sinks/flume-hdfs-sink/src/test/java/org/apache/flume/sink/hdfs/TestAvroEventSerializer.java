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
package org.apache.flume.sink.hdfs;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.serialization.EventSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestAvroEventSerializer {

  private File file;

  @Before
  public void setUp() throws Exception {
    file = File.createTempFile(getClass().getSimpleName(), "");
  }

  @Test
  public void testNoCompression() throws IOException {
    createAvroFile(file, null, false);
    validateAvroFile(file);
  }

  @Test
  public void testNullCompression() throws IOException {
    createAvroFile(file, "null", false);
    validateAvroFile(file);
  }

  @Test
  public void testDeflateCompression() throws IOException {
    createAvroFile(file, "deflate", false);
    validateAvroFile(file);
  }

  @Test
  public void testSnappyCompression() throws IOException {
    createAvroFile(file, "snappy", false);
    validateAvroFile(file);
  }

  @Test
  public void testSchemaUrl() throws IOException {
    createAvroFile(file, null, true);
    validateAvroFile(file);
  }

  public void createAvroFile(File file, String codec, boolean useSchemaUrl) throws
      IOException {

    // serialize a few events using the reflection-based avro serializer
    OutputStream out = new FileOutputStream(file);

    Context ctx = new Context();
    if (codec != null) {
      ctx.put("compressionCodec", codec);
    }

    Schema schema = Schema.createRecord("myrecord", null, null, false);
    schema.setFields(Arrays.asList(new Schema.Field[]{
        new Schema.Field("message", Schema.create(Schema.Type.STRING), null, null)
    }));
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
    File schemaFile = null;
    if (useSchemaUrl) {
      schemaFile = File.createTempFile(getClass().getSimpleName(), ".avsc");
      Files.write(schema.toString(), schemaFile, Charsets.UTF_8);
    }

    EventSerializer.Builder builder = new AvroEventSerializer.Builder();
    EventSerializer serializer = builder.build(ctx, out);

    serializer.afterCreate();
    for (int i = 0; i < 3; i++) {
      GenericRecord record = recordBuilder.set("message", "Hello " + i).build();
      Event event = EventBuilder.withBody(serializeAvro(record, schema));
      if (schemaFile == null) {
        event.getHeaders().put(AvroEventSerializer.AVRO_SCHEMA_LITERAL_HEADER,
            schema.toString());
      } else {
        event.getHeaders().put(AvroEventSerializer.AVRO_SCHEMA_URL_HEADER,
            schemaFile.toURI().toURL().toExternalForm());
      }
      serializer.write(event);
    }
    serializer.flush();
    serializer.beforeClose();
    out.flush();
    out.close();
  }

  private byte[] serializeAvro(Object datum, Schema schema) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ReflectDatumWriter<Object> writer = new ReflectDatumWriter<Object>(schema);
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
    out.reset();
    writer.write(datum, encoder);
    encoder.flush();
    return out.toByteArray();
  }

  public void validateAvroFile(File file) throws IOException {
    // read the events back using GenericRecord
    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
    DataFileReader<GenericRecord> fileReader =
        new DataFileReader<GenericRecord>(file, reader);
    GenericRecord record = new GenericData.Record(fileReader.getSchema());
    int numEvents = 0;
    while (fileReader.hasNext()) {
      fileReader.next(record);
      String bodyStr = record.get("message").toString();
      System.out.println(bodyStr);
      numEvents++;
    }
    fileReader.close();
    Assert.assertEquals("Should have found a total of 3 events", 3, numEvents);
  }
}
