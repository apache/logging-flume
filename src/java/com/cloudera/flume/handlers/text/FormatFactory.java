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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public abstract String getName();

    public abstract OutputFormat build(String... args);
  };

  final static OutputFormatBuilder DEFAULT = new OutputFormatBuilder() {
    public OutputFormat build(String... args) {
      return new RawOutputFormat();
    }

    @Override
    public String getName() {
      return "default";
    }
  };

  final static Map<String, OutputFormatBuilder> CORE_FORMATS = new HashMap<String, OutputFormatBuilder>() {
    {
      put(DEFAULT.getName(), DEFAULT);
      put(DebugOutputFormat.builder().getName(), DebugOutputFormat.builder());
      put(RawOutputFormat.builder().getName(), RawOutputFormat.builder());
      put(SyslogEntryFormat.builder().getName(), SyslogEntryFormat.builder());
      put(Log4jOutputFormat.builder().getName(), Log4jOutputFormat.builder());
      put(AvroJsonOutputFormat.builder().getName(), AvroJsonOutputFormat.builder());
      put(AvroDataFileOutputFormat.builder().getName(), AvroDataFileOutputFormat.builder());
      put(AvroNativeFileOutputFormat.builder().getName(), AvroNativeFileOutputFormat.builder());
    }
  };

  private static final Logger logger = LoggerFactory
      .getLogger(FormatFactory.class);

  // order matters here. This has to be after FORMATS
  final static FormatFactory defaultfactory = new FormatFactory();

  Map<String, OutputFormatBuilder> registeredFormats;

  FormatFactory(Map<String, OutputFormatBuilder> formats) {
    this.registeredFormats = formats;
  }

  public FormatFactory() {
    this(CORE_FORMATS);
  }

  public OutputFormat getOutputFormat(String name, String... args)
      throws FlumeSpecException {
    OutputFormatBuilder builder;

    // purposely said no format? return default.
    if (name == null) {
      return DEFAULT.build();
    }

    // specified a format but it was invalid? This is a problem.
    synchronized (registeredFormats) {
      builder = registeredFormats.get(name);
    }

    if (builder == null) {
      throw new FlumeSpecException("Invalid output format: " + name);
    }

    return builder.build(args);
  }

  public static FormatFactory get() {
    return defaultfactory;
  }

  /**
   * Register {@link OutputFormatBuilder} with the name {@code name}. This is
   * use internally by {@link #registerFormat(OutputFormatBuilder)} and when one
   * would like to create alias names for an {@link OutputFormatBuilder}.
   * 
   * @param name
   *          The name for the output format plugin.
   * @param builder
   *          The builder to register.
   * @return {@code this} to permit method chaining.
   * @throws FlumeSpecException
   *           If a {@code builder} is already registered for {@code name}.
   */
  public FormatFactory registerFormat(String name, OutputFormatBuilder builder) throws FlumeSpecException {
    synchronized (registeredFormats) {
      if (registeredFormats.containsKey(name)) {
        throw new FlumeSpecException("Redefining registered output formats is not permitted. Attempted to redefine " + name);
      }

      registeredFormats.put(name, builder);
    }

    return this;
  }

  /**
   * Register an {@link OutputFormatBuilder} with the {@link FormatFactory}.
   * 
   * @param builder
   *          The builder to register.
   * @return {@code this} to permit method chaining.
   * @throws FlumeSpecException
   *           If the {@code builder} is already registered.
   */
  public FormatFactory registerFormat(OutputFormatBuilder builder) throws FlumeSpecException {
    synchronized (registeredFormats) {
      String name;

      name = builder.getName();

      registerFormat(name, builder);
    }

    return this;
  }

  /**
   * Returns a copy of the registered formats at the time of invocation.
   * 
   * @return
   */
  public Collection<OutputFormatBuilder> getRegisteredFormats() {
    synchronized (registeredFormats) {
      return new ArrayList<FormatFactory.OutputFormatBuilder>(registeredFormats.values());
    }
  }

  /**
   * Remove {@code formatName} from the registry of output formats. Note that
   * this does not attempt to unload the class even if it was dynamically
   * loaded.
   * 
   * @param formatName The format name to unregister
   */
  public boolean unregisterFormat(String formatName) {
    synchronized (registeredFormats) {
      if (registeredFormats.containsKey(formatName)) {
        return registeredFormats.remove(formatName) != null;
      } else {
        logger.warn("Attempt to unregister an unknown format " + formatName);
      }
    }

    return false;
  }

}
