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
package com.cloudera.flume.handlers.hdfs;

import static org.junit.Assert.assertEquals;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Test;

/**
 * Tests methods of {@link CustomDfsSink}.
 */
public class TestCustomDfsSink {

  private void checkCodec(Class<?> c, String codecName) {
    FlumeConfiguration conf = FlumeConfiguration.get();
    CompressionCodec codec = CustomDfsSink.getCodec(conf, codecName);
    assertEquals(c, codec.getClass());
  }
  @Test
  public void testGetCodec() {
    checkCodec(GzipCodec.class, "gzip");
    checkCodec(GzipCodec.class, "Gzip");
    checkCodec(GzipCodec.class, "gzipcodec");
    checkCodec(GzipCodec.class, "GzipCodec");
    checkCodec(GzipCodec.class, "org.apache.hadoop.io.compress.GzipCodec");
  }

  @Test
  public void testOutputFormats() throws FlumeSpecException {
    // format
    String src = "formatDfs(\"file:///tmp/test/testfilename\", avro)";
    FlumeBuilder.buildSink(new Context(), src);

    // format
    src = "formatDfs(\"file:///tmp/test/testfilename\", seqfile)";
    FlumeBuilder.buildSink(new Context(), src);

    // format
    src = "formatDfs(\"file:///tmp/test/testfilename\", seqfile(\"bzip2\"))";
    FlumeBuilder.buildSink(new Context(), src);
  }

}
