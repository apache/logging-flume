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
package org.apache.flume.serialization;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import com.google.common.base.Charsets;

public class TestFlumeEventAvroEventSerializer {

  private static final File TESTFILE =
      new File("src/test/resources/FlumeEventAvroEvent.avro");

  @Test
  public void testAvroSerializer()
      throws FileNotFoundException, IOException {

    createAvroFile(TESTFILE, null);
    validateAvroFile(TESTFILE);
    FileUtils.forceDelete(TESTFILE);
  }

  @Test
  public void testAvroSerializerNullCompression()
      throws FileNotFoundException, IOException {

    createAvroFile(TESTFILE, "null");
    validateAvroFile(TESTFILE);
    FileUtils.forceDelete(TESTFILE);
  }

  @Test
  public void testAvroSerializerDeflateCompression()
      throws FileNotFoundException, IOException {

    createAvroFile(TESTFILE, "deflate");
    validateAvroFile(TESTFILE);
    FileUtils.forceDelete(TESTFILE);
  }

  @Test
  public void testAvroSerializerSnappyCompression()
      throws FileNotFoundException, IOException {
    // Snappy currently broken on Mac in OpenJDK 7 per FLUME-2012
    Assume.assumeTrue(!"Mac OS X".equals(System.getProperty("os.name")) ||
      !System.getProperty("java.version").startsWith("1.7."));

    createAvroFile(TESTFILE, "snappy");
    validateAvroFile(TESTFILE);
    FileUtils.forceDelete(TESTFILE);
  }

  public void createAvroFile(File file, String codec)
      throws FileNotFoundException, IOException {

    if(file.exists()){
      FileUtils.forceDelete(file);
    }

    // serialize a few events using the reflection-based avro serializer
    OutputStream out = new FileOutputStream(file);

    Context ctx = new Context();
    if (codec != null) {
      ctx.put("compressionCodec", codec);
    }

    EventSerializer.Builder builder =
        new FlumeEventAvroEventSerializer.Builder();
    EventSerializer serializer = builder.build(ctx, out);

    serializer.afterCreate();
    serializer.write(EventBuilder.withBody("yo man!", Charsets.UTF_8));
    serializer.write(EventBuilder.withBody("2nd event!", Charsets.UTF_8));
    serializer.write(EventBuilder.withBody("last one!", Charsets.UTF_8));
    serializer.flush();
    serializer.beforeClose();
    out.flush();
    out.close();
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
      ByteBuffer body = (ByteBuffer) record.get("body");
      CharsetDecoder decoder = Charsets.UTF_8.newDecoder();
      String bodyStr = decoder.decode(body).toString();
      System.out.println(bodyStr);
      numEvents++;
    }
    fileReader.close();
    Assert.assertEquals("Should have found a total of 3 events", 3, numEvents);
  }

}
