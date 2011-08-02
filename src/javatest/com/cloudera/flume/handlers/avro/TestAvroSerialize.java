/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.handlers.avro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.log4j.Logger;

import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.Event.Priority;

/**
 * Tests to see how Avro serialization/deserialization works.
 */
public class TestAvroSerialize extends TestCase {

  final static Logger LOG = Logger.getLogger(TestAvroSerialize.class.getName());

  public void setUp() {
    LOG.info("----");
  }

  /**
   * Experiment to see what a schema looks like.
   */
  public void testEventSchema() throws IOException {
    ReflectData reflectData = ReflectData.get();
    Schema schm = reflectData.getSchema(EventImpl.class);
    LOG.info(schm);
  }

  /**
   * Simple test class that is similar to an EventImpl.
   */
  public static class A {
    byte[] body;

    long timestamp;
    Priority pri;
    long nanos;
    String host;

  };

  /**
   * An instance initialized with some specific values.
   */
  static A anA = new A() {
    {
      body = "foo".getBytes();
      timestamp = 1234;
      pri = Priority.INFO;
      host = "bar";
    }
  };

  /**
   * Helper to dump bytes similar to 'od -xab'
   */
  public void dump(byte[] bs) {
    int len = bs.length;
    LOG.info("output size: " + len);

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < len; i++) {
      sb.append(String.format("%02x ", bs[i]));
    }
    LOG.info(sb);

    StringBuilder sb2 = new StringBuilder();
    for (int i = 0; i < len; i++) {
      char c = !Character.isISOControl(bs[i]) ? (char) bs[i] : (char) '?';
      sb2.append(String.format("%2c ", c));
    }
    LOG.info(sb2);
  }

  /**
   * Try BinaryEncoder
   */
  public void testSerializeBinary() throws IOException {
    ReflectData reflectData = ReflectData.get();
    Schema schm = reflectData.getSchema(A.class);
    LOG.info(schm);

    ReflectDatumWriter<A> writer = new ReflectDatumWriter<A>(schm);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder json = new BinaryEncoder(out);
    writer.write(anA, json);

    byte[] bs = out.toByteArray();
    dump(bs);
    assertEquals(12, bs.length);

    ByteArrayInputStream bais = new ByteArrayInputStream(bs);
    ReflectDatumReader<A> reader = new ReflectDatumReader<A>(schm);
    BinaryDecoder dec = (new DecoderFactory()).createBinaryDecoder(bais, null);
    A decoded = reader.read(null, dec);
    LOG.info(decoded);
  }

  /**
   * Try JsonEncoder
   */
  public void testSerializeJson() throws IOException {
    ReflectData reflectData = ReflectData.get();
    Schema schm = reflectData.getSchema(A.class);
    LOG.info(schm);

    ReflectDatumWriter<A> writer = new ReflectDatumWriter<A>(schm);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonEncoder json = new JsonEncoder(schm, out);
    writer.write(anA, json);

    byte[] bs = out.toByteArray();
    int len = bs.length;
    LOG.info("output size: " + len);
    assertEquals(0, bs.length); // This is strange!

    json.flush(); // there should be a ReflectDatumWriter.flush();
    bs = out.toByteArray();
    dump(bs);
    assertEquals(67, bs.length);

    ByteArrayInputStream bais = new ByteArrayInputStream(bs);
    ReflectDatumReader<A> reader = new ReflectDatumReader<A>(schm);
    Object decoded = reader.read(null, new JsonDecoder(schm, bais));
    LOG.info(decoded);
  }

  /**
   * Instantiate an event with some values in fields.
   */
  static EventImpl e = new EventImpl("blah".getBytes(), 0, Priority.INFO, 0,
      "hostname") {
    {
      Attributes.setString(this, "stringAttr", "stringValue");
      Attributes.setLong(this, "longAttr", 0xdeadbeef);
    }
  };

  /**
   * JsonEncoder and JsonDecoder on a EventImpl
   */
  public void testEventSchemaSerializeJson() throws IOException {
    ReflectData reflectData = ReflectData.get();
    Schema schm = reflectData.getSchema(EventImpl.class);
    LOG.info(schm);

    ReflectDatumWriter<EventImpl> writer = new ReflectDatumWriter<EventImpl>(
        schm);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonEncoder json = new JsonEncoder(schm, out);
    writer.write(e, json);
    json.flush();
    byte[] bs = out.toByteArray();
    dump(bs);
    assertEquals(138, bs.length);

    ByteArrayInputStream bais = new ByteArrayInputStream(bs);
    ReflectDatumReader<EventImpl> reader = new ReflectDatumReader<EventImpl>(
        schm);
    EventImpl decoded = reader.read(null, new JsonDecoder(schm, bais));
    LOG.info(decoded);
  }

  /**
   * BinaryEnconder and BinaryDecoder on a EventImpl
   */
  public void testEventSchemaSerializeBin() throws IOException {
    ReflectData reflectData = ReflectData.get();
    Schema schm = reflectData.getSchema(EventImpl.class);
    LOG.info(schm);

    ReflectDatumWriter<EventImpl> writer = new ReflectDatumWriter<EventImpl>(
        schm);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    writer.write(e, new BinaryEncoder(out));

    byte[] bs = out.toByteArray();
    dump(bs);
    assertEquals(60, bs.length);

    ByteArrayInputStream bais = new ByteArrayInputStream(bs);
    ReflectDatumReader<EventImpl> reader = new ReflectDatumReader<EventImpl>(
        schm);
    BinaryDecoder dec = (new DecoderFactory()).createBinaryDecoder(bais, null);
    EventImpl decoded = reader.read(null, dec);
    // EventImpl decoded = reader.read(null, new BinaryDecoder(bais));
    LOG.info(decoded);
  }

}
