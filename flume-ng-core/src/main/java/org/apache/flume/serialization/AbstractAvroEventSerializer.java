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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flume.serialization.AvroEventSerializerConfigurationConstants.*;

/**
 * This is a helper class provided to make it straightforward to serialize
 * Flume {@linkplain Event events} into Avro data.
 * @param <T> Data type that can be written in the Schema given below.
 */
public abstract class AbstractAvroEventSerializer<T>
    implements EventSerializer, Configurable {

  private static final Logger logger =
      LoggerFactory.getLogger(AbstractAvroEventSerializer.class);

  private DatumWriter<T> writer = null;
  private DataFileWriter<T> dataFileWriter = null;

  /**
   * Returns the stream to serialize data into.
   */
  protected abstract OutputStream getOutputStream();

  /**
   * Returns the parsed Avro schema corresponding to the data being written
   * and the parameterized type specified.
   */
  protected abstract Schema getSchema();

  /**
   * Simple conversion routine used to convert an Event to a type of your
   * choosing. That type must correspond to the Avro schema given by
   * {@link #getSchema()}.
   */
  protected abstract T convert(Event event);

  @Override
  public void configure(Context context) {

    int syncIntervalBytes =
        context.getInteger(SYNC_INTERVAL_BYTES, DEFAULT_SYNC_INTERVAL_BYTES);
    String compressionCodec =
        context.getString(COMPRESSION_CODEC, DEFAULT_COMPRESSION_CODEC);

    writer = new ReflectDatumWriter<T>(getSchema());
    dataFileWriter = new DataFileWriter<T>(writer);

    dataFileWriter.setSyncInterval(syncIntervalBytes);

    try {
      CodecFactory codecFactory = CodecFactory.fromString(compressionCodec);
      dataFileWriter.setCodec(codecFactory);
    } catch (AvroRuntimeException e) {
      logger.warn("Unable to instantiate avro codec with name (" +
          compressionCodec + "). Compression disabled. Exception follows.", e);
    }
  }

  @Override
  public void afterCreate() throws IOException {
    // write the AVRO container format header
    dataFileWriter.create(getSchema(), getOutputStream());
  }

  @Override
  public void afterReopen() throws IOException {
    // impossible to initialize DataFileWriter without writing the schema?
    throw new UnsupportedOperationException("Avro API doesn't support append");
  }

  @Override
  public void write(Event event) throws IOException {
    T destType = convert(event);
    dataFileWriter.append(destType);
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

}
