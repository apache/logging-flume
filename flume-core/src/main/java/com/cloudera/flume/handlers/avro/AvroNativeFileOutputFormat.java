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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.handlers.text.FormatFactory.OutputFormatBuilder;
import com.cloudera.flume.handlers.text.output.AbstractOutputFormat;
import com.cloudera.flume.handlers.text.output.OutputFormat;
import com.google.common.base.Preconditions;

/**
 * This writes native Avro formatted files out as an output format.
 * 
 * Note: There is a separate Avro container that does encoding currently from
 * the AvroEventSource/Sinks. A separate patch will consolidate the two.
 */
public class AvroNativeFileOutputFormat extends AbstractOutputFormat {

  private static final String formatName = "avro";

  final static ReflectData reflectData = ReflectData.get();
  final static Schema schema = reflectData.getSchema(EventImpl.class);

  ReflectDatumWriter<EventImpl> dw = new ReflectDatumWriter<EventImpl>(schema);
  OutputStream cachedOut = null;
  DataFileWriter<EventImpl> enc = new DataFileWriter<EventImpl>(dw);

  @Override
  public void format(OutputStream o, Event e) throws IOException {
    if (cachedOut == null) {
      // first time, no current OutputStream
      enc.create(schema, o);
      cachedOut = o;
    }

    if (cachedOut != o) {
      // different output file than last time
      enc.close();

      enc.create(schema, o);
      cachedOut = o;
    }

    EventImpl ei = null;
    if (e instanceof EventImpl) {
      ei = (EventImpl) e;
    } else {
      // copy constructor to force into an EventImpl for reflection
      ei = new EventImpl(e);
    }

    enc.append(ei);
    enc.flush();
  }

  public void close() throws IOException {
    enc.close();
  }

  public static OutputFormatBuilder builder() {
    return new OutputFormatBuilder() {
      @Override
      public OutputFormat build(String... args) {
        Preconditions.checkArgument(args.length == 0, "usage: avro");

        OutputFormat format = new AvroNativeFileOutputFormat();
        format.setBuilder(this);

        return format;
      }

      @Override
      public String getName() {
        return formatName;
      }

    };
  }

}
