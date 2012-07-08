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

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.handlers.text.FormatFactory.OutputFormatBuilder;
import com.cloudera.flume.handlers.text.output.OutputFormat;

public class TestFormatFactory {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestFormatFactory.class);

  /**
   * Test the output format plugin loading mechanism. We additionally test that
   * the builtin output formats continue to work.
   */
  @Test
  public void testOutputFormatPluginLoader() {
    FlumeConfiguration.get().set(
        FlumeConfiguration.OUTPUT_FORMAT_PLUGIN_CLASSES, "java.lang.String");
    FormatFactory.loadOutputFormatPlugins();

    try {
      Assert.assertNotNull(FlumeBuilder.buildSink(new Context(),
          "console(\"raw\")"));
      Assert.assertNotNull(FlumeBuilder.buildSink(new Context(),
          "console(\"avrojson\")"));
      Assert.assertNotNull(FlumeBuilder.buildSink(new Context(),
          "console(\"avrodata\")"));
      Assert.assertNotNull(FlumeBuilder.buildSink(new Context(),
          "console(\"syslog\")"));
      Assert.assertNotNull(FlumeBuilder.buildSink(new Context(),
          "console(\"log4j\")"));
      Assert.assertNotNull(FlumeBuilder.buildSink(new Context(), "console()"));
    } catch (FlumeSpecException e) {
      LOG.error(
          "Unable to create a console sink with one of the built in output formats. Exception follows.",
          e);
      Assert
          .fail("Unable to create a console sink with one of the built in output formats - "
              + e.getMessage());
    }
  }

  @Test
  public void testCustomOutputPluginLoader() {
    FlumeConfiguration
        .get()
        .set(FlumeConfiguration.OUTPUT_FORMAT_PLUGIN_CLASSES,
            "com.cloudera.flume.handlers.text.TestFormatFactory$TestOutputFormatPlugin");
    FormatFactory.loadOutputFormatPlugins();

    try {
      FlumeBuilder.buildSink(new Context(), "console(\"testformat\")");
    } catch (FlumeSpecException e) {
      LOG.error(
          "Caught exception building console sink with testformat output format. Exception follows.",
          e);
      Assert
          .fail("Unable to build a console sink with testformat output format");
    }

    /* Manually reset the registered plugins as best we can. */
    Assert.assertTrue(FormatFactory.get().unregisterFormat("testformat"));
  }

  public static class TestOutputFormatPlugin extends OutputFormatBuilder {

    @Override
    public OutputFormat build(String... args) {
      // Do nothing.
      return null;
    }

    public String getName() {
      return "testformat";
    }

  }
}
