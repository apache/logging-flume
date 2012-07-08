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
package com.cloudera.flume.hbase;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.LogicalNodeContext;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.conf.SinkFactoryImpl;
import com.cloudera.flume.hbase.HBaseSink.QualifierSpec;

/**
 * Test the hbase sink writes events to a table/family properly
 */
public class TestHBaseSinkBuilder {
  public static Logger LOG = LoggerFactory
      .getLogger(TestHBaseSinkBuilder.class);

  @BeforeClass
  public static void setup() throws Exception {
    SinkFactoryImpl sf = new SinkFactoryImpl();
    sf.setSink("hbase", HBaseSink.builder());
    FlumeBuilder.setSinkFactory(sf);
  }

  /**
   * Expect no failures
   */
  @Test
  public void testBuilder() {
    SinkBuilder esb = HBaseSink.builder();
    HBaseSink snk = (HBaseSink) esb.build(LogicalNodeContext.testingContext(),
        "table", "%{rowkey}", "cf1", "col1", "%{attr1}", "cf2", "col2",
        "%{attr2}");
    assertEquals(snk.tableName, "table");
    assertEquals(snk.rowkey, "%{rowkey}");

    QualifierSpec s0 = snk.spec.get(0);
    assertEquals(s0.colFam, "cf1");
    assertEquals(s0.col, "col1");
    assertEquals(s0.value, "%{attr1}");

    QualifierSpec s1 = snk.spec.get(1);
    assertEquals(s1.colFam, "cf2");
    assertEquals(s1.col, "col2");
    assertEquals(s1.value, "%{attr2}");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailNotTriples() {
    SinkBuilder esb = HBaseSink.builder();
    HBaseSink snk = (HBaseSink) esb.build(LogicalNodeContext.testingContext(),
        "table", "%{rowkey}", "cf1", "col1", "%{attr1}", "cf2", "col2");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailNotEnough() {
    SinkBuilder esb = HBaseSink.builder();
    HBaseSink snk = (HBaseSink) esb.build(LogicalNodeContext.testingContext(),
        "table");
  }

  @Test
  public void testKwArgs() throws FlumeSpecException {
    HBaseSink s = (HBaseSink) FlumeBuilder.buildSink(
        LogicalNodeContext.testingContext(), "hbase(\"table\","
            + "\"%{rowkey}\"," + "\"cf1\", \"col1\", \"%{attr1}\","
            + "writeBufferSize=10000, writeToWal=true)");
    assertEquals(s.writeBufferSize, 10000);
    assertEquals(s.writeToWal, true);

    s = (HBaseSink) FlumeBuilder.buildSink(LogicalNodeContext.testingContext(),
        "hbase(\"table\"," + "\"%{rowkey}\","
            + "\"cf1\", \"col1\", \"%{attr1}\"," + "writeToWal=false)");
    assertEquals(s.writeBufferSize, 0);
    assertEquals(s.writeToWal, false);

    s = (HBaseSink) FlumeBuilder.buildSink(LogicalNodeContext.testingContext(),
        "hbase(\"table\"," + "\"%{rowkey}\","
            + "\"cf1\", \"col1\", \"%{attr1}\"," + "writeBufferSize=10000)");
    assertEquals(s.writeBufferSize, 10000);
    assertEquals(s.writeToWal, false);
  }
}
