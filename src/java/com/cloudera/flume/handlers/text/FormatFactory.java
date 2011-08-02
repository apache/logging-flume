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
package com.cloudera.flume.handlers.text;

import java.util.HashMap;
import java.util.Map;

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.handlers.avro.AvroDataFileOutputFormat;
import com.cloudera.flume.handlers.avro.AvroJsonOutputFormat;
import com.cloudera.flume.handlers.avro.AvroNativeFileOutputFormat;
import com.cloudera.flume.handlers.text.output.DebugOutputFormat;
import com.cloudera.flume.handlers.text.output.Log4jOutputFormat;
import com.cloudera.flume.handlers.text.output.OutputFormat;
import com.cloudera.flume.handlers.text.output.RawOutputFormat;

/**
 * This is a factory that produces OutputFormats, give a name and a list of
 * string arguments.
 * 
 * These are setup this way so that later we can do Formats plugins that are
 * auto-detected using reflection.
 */
@SuppressWarnings("serial")
public class FormatFactory {

  public abstract static class OutputFormatBuilder {
    public abstract OutputFormat build(String... args);
  };

  final static OutputFormatBuilder DEFAULT = new OutputFormatBuilder() {
    public OutputFormat build(String... args) {
      return new RawOutputFormat();
    }
  };

  final static Map<String, OutputFormatBuilder> FORMATS = new HashMap<String, OutputFormatBuilder>() {
    {
      put("default", DEFAULT);
      put("debug", DebugOutputFormat.builder());
      put("raw", RawOutputFormat.builder());
      put("syslog", SyslogEntryFormat.builder());
      put("log4j", Log4jOutputFormat.builder());
      put("avrojson", AvroJsonOutputFormat.builder());
      put("avrodata", AvroDataFileOutputFormat.builder());
      put("avro", AvroNativeFileOutputFormat.builder());
    }
  };

  // order matters here. This has to be after FORMATS
  final static FormatFactory defaultfactory = new FormatFactory();

  Map<String, OutputFormatBuilder> outputs;

  FormatFactory(Map<String, OutputFormatBuilder> formats) {
    this.outputs = formats;
  }

  public FormatFactory() {
    this(FORMATS);
  }

  public OutputFormat getOutputFormat(String name, String... args)
      throws FlumeSpecException {

    // purposely said no format? return default.
    if (name == null) {
      return DEFAULT.build();
    }

    // specified a format but it was invalid? This is a problem.
    OutputFormatBuilder builder = outputs.get(name);
    if (builder == null) {
      throw new FlumeSpecException("Invalid output format: " + name);
    }

    return builder.build(args);
  }

  public static FormatFactory get() {
    return defaultfactory;
  }

}
