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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.serialization.EventSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.*;

/**
 * <p>
 * This class serializes Flume {@linkplain org.apache.flume.Event events} into Avro data files. The
 * Flume event body is read as an Avro datum, and is then written to the
 * {@link org.apache.flume.serialization.EventSerializer}'s output stream in Avro data file format.
 * </p>
 * <p>
 * The Avro schema is determined by reading a Flume event header. The schema may be
 * specified either as a literal, by setting {@link #AVRO_SCHEMA_LITERAL_HEADER} (not
 * recommended, since the full schema must be transmitted in every event),
 * or as a URL which the schema may be read from, by setting {@link
 * #AVRO_SCHEMA_URL_HEADER}. Schemas read from URLs are cached by instances of this
 * class so that the overhead of retrieval is minimized.
 * </p>
 */
public class AvroEventSerializer implements EventSerializer, Configurable {

  private static final Logger logger =
      LoggerFactory.getLogger(AvroEventSerializer.class);

  public static final String AVRO_SCHEMA_LITERAL_HEADER = "flume.avro.schema.literal";
  public static final String AVRO_SCHEMA_URL_HEADER = "flume.avro.schema.url";

  private final OutputStream out;
  private DatumWriter<Object> writer = null;
  private DataFileWriter<Object> dataFileWriter = null;

  private int syncIntervalBytes;
  private String compressionCodec;
  private Map<String, Schema> schemaCache = new HashMap<String, Schema>();

  private AvroEventSerializer(OutputStream out) {
    this.out = out;
  }

  @Override
  public void configure(Context context) {
    syncIntervalBytes =
        context.getInteger(SYNC_INTERVAL_BYTES, DEFAULT_SYNC_INTERVAL_BYTES);
    compressionCodec =
        context.getString(COMPRESSION_CODEC, DEFAULT_COMPRESSION_CODEC);
  }

  @Override
  public void afterCreate() throws IOException {
    // no-op
  }

  @Override
  public void afterReopen() throws IOException {
    // impossible to initialize DataFileWriter without writing the schema?
    throw new UnsupportedOperationException("Avro API doesn't support append");
  }

  @Override
  public void write(Event event) throws IOException {
    if (dataFileWriter == null) {
      initialize(event);
    }
    dataFileWriter.appendEncoded(ByteBuffer.wrap(event.getBody()));
  }

  private void initialize(Event event) throws IOException {
    Schema schema = null;
    String schemaUrl = event.getHeaders().get(AVRO_SCHEMA_URL_HEADER);
    if (schemaUrl != null) {
      schema = schemaCache.get(schemaUrl);
      if (schema == null) {
        schema = loadFromUrl(schemaUrl);
        schemaCache.put(schemaUrl, schema);
      }
    }
    if (schema == null) {
      String schemaString = event.getHeaders().get(AVRO_SCHEMA_LITERAL_HEADER);
      if (schemaString == null) {
        throw new FlumeException("Could not find schema for event " + event);
      }
      schema = new Schema.Parser().parse(schemaString);
    }

    writer = new GenericDatumWriter<Object>(schema);
    dataFileWriter = new DataFileWriter<Object>(writer);

    dataFileWriter.setSyncInterval(syncIntervalBytes);

    try {
      CodecFactory codecFactory = CodecFactory.fromString(compressionCodec);
      dataFileWriter.setCodec(codecFactory);
    } catch (AvroRuntimeException e) {
      logger.warn("Unable to instantiate avro codec with name (" +
          compressionCodec + "). Compression disabled. Exception follows.", e);
    }

    dataFileWriter.create(schema, out);
  }

  private Schema loadFromUrl(String schemaUrl) throws IOException {
    Configuration conf = new Configuration();
    Schema.Parser parser = new Schema.Parser();
    if (schemaUrl.toLowerCase().startsWith("hdfs://")) {
      FileSystem fs = FileSystem.get(conf);
      FSDataInputStream input = null;
      try {
        input = fs.open(new Path(schemaUrl));
        return parser.parse(input);
      } finally {
        if (input != null) {
          input.close();
        }
      }
    } else {
      InputStream is = null;
      try {
        is = new URL(schemaUrl).openStream();
        return parser.parse(is);
      } finally {
        if (is != null) {
          is.close();
        }
      }
    }
  }

  @Override
  public void flush() throws IOException {
    dataFileWriter.flush();
  }

  @Override
  public void beforeClose() throws IOException {
    // no-op
  }

  @Override
  public boolean supportsReopen() {
    return false;
  }

  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(Context context, OutputStream out) {
      AvroEventSerializer writer = new AvroEventSerializer(out);
      writer.configure(context);
      return writer;
    }

  }

}
