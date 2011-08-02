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
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.handlers.text.FormatFactory.OutputFormatBuilder;
import com.cloudera.flume.handlers.text.output.OutputFormat;
import com.google.common.base.Preconditions;

/**
 * This uses the Avro's Reflection serializer and DataFileWriter to output Flume
 * events to a AvroDataFile. Under the hood this uses the BinaryEncoder for
 * efficient storage and also as another wrapper layer for partitioning blocks
 * that allow for splitting.
 * 
 * This is not thread safe.
 */
public class AvroDataFileOutputFormat implements OutputFormat {
  final static ReflectData reflectData = ReflectData.get();
  final static Schema schema = reflectData.getSchema(EventImpl.class);

  DatumWriter<EventImpl> writer = new ReflectDatumWriter<EventImpl>(schema);

  DataFileWriter<EventImpl> sink = null;
  OutputStream cachedOut = null;

  public AvroDataFileOutputFormat() {
  }

  @Override
  public void format(OutputStream o, Event e) throws IOException {
    if (sink == null) {
      // first time, no current OutputStream or sink.
      cachedOut = o;
      sink = new DataFileWriter<EventImpl>(writer);
      sink.create(schema, o); // this opens
    }

    if (cachedOut != o) {
      // different output than last time, fail here
      throw new IOException(
          "OutputFormat instance can only write to the same OutputStream");
    }

    EventImpl ei = null;
    if (e instanceof EventImpl) {
      ei = (EventImpl) e;
    } else {
      // copy constructor to force into an EventImpl for reflection
      ei = new EventImpl(e);
    }
    sink.append(ei);
    sink.flush();
  }

  @Override
  public String getFormatName() {
    return "avrodatafile";
  }

  public static OutputFormatBuilder builder() {
    return new OutputFormatBuilder() {
      @Override
      public OutputFormat build(String... args) {
        Preconditions.checkArgument(args.length == 0, "usage: avrodata");
        return new AvroDataFileOutputFormat();
      }

    };
  }

}
