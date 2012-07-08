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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.Event.Priority;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.hbase.HBaseSink.QualifierSpec;
import com.cloudera.util.Clock;

/**
 * Test the hbase sink writes events to a table/family properly
 */
public class TestHBaseSink {
  private static HBaseTestEnv hbaseEnv;
  public static Logger LOG = LoggerFactory.getLogger(TestHBaseSink.class);
  public static final String DEFAULT_HOST = "qwigibo";

  @BeforeClass
  public static void setup() throws Exception {
    // expensive, so just do it once for all tests, just make sure
    // that tests don't overlap (use diff tables for each test)
    hbaseEnv = new HBaseTestEnv();
    hbaseEnv.conf.set(HBaseTestCase.TEST_DIRECTORY_KEY, "build/test/data");
    hbaseEnv.setUp();
  }

  @AfterClass
  public static void teardown() throws Exception {
    hbaseEnv.tearDown();
  }

  void shipThreeEvents(HBaseSink snk) throws IOException {
    snk.open();
    try {
      Event e1 = new EventImpl("message0".getBytes(), Clock.unixTime(),
          Priority.INFO, 0, DEFAULT_HOST);
      e1.set("rowkey", Bytes.toBytes("row-key0"));
      e1.set("attr1", Bytes.toBytes("attr1_val0"));
      e1.set("attr2", Bytes.toBytes("attr2_val0"));
      e1.set("other", Bytes.toBytes("other_val0"));
      snk.append(e1);

      Event e2 = new EventImpl("message1".getBytes(), Clock.unixTime(),
          Priority.INFO, 1, DEFAULT_HOST);
      e2.set("rowkey", Bytes.toBytes("row-key1"));
      e2.set("attr1", Bytes.toBytes("attr1_val1"));
      e2.set("attr2", Bytes.toBytes("attr2_val1"));
      e2.set("other", Bytes.toBytes("other_val1"));
      snk.append(e2);

      Event e3 = new EventImpl("message2".getBytes(), Clock.unixTime(),
          Priority.INFO, 2, DEFAULT_HOST);
      e3.set("rowkey", Bytes.toBytes("row-key2"));
      e3.set("attr1", Bytes.toBytes("attr1_val2"));
      e3.set("attr2", Bytes.toBytes("attr2_val2"));
      e3.set("other", Bytes.toBytes("other_val2"));
      snk.append(e3);
    } finally {
      snk.close();
    }
  }

  /**
   * Write events to a sink directly, verify by scanning HBase table. x
   */
  @Test
  public void testSink() throws IOException, InterruptedException {
    final String tableName = "testSink";
    final String tableFamily1 = "family1";
    final String tableFamily2 = "family2";

    // create the table and column family to be used by sink
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(tableFamily1));
    desc.addFamily(new HColumnDescriptor(tableFamily2));
    HBaseAdmin admin = new HBaseAdmin(hbaseEnv.conf);
    admin.createTable(desc);

    // explicit constructor rather than builder - we want to control the conf
    List<QualifierSpec> spec = new ArrayList<QualifierSpec>();
    spec.add(new QualifierSpec(tableFamily1, "col1", "%{attr1}"));
    spec.add(new QualifierSpec(tableFamily2, "col2", "%{attr2}"));
    HBaseSink snk = new HBaseSink(tableName, "%{rowkey}", spec, 0L, false,
        hbaseEnv.conf);
    shipThreeEvents(snk);

    // verify that the events made it into hbase
    HTable table = new HTable(hbaseEnv.conf, tableName);
    try {
      for (long i = 0; i <= 2; i++) {
        Result r = table.get(new Get(Bytes.toBytes("row-key" + i)));
        LOG.info("result " + r);

        byte[] v1 = r.getValue(Bytes.toBytes(tableFamily1),
            Bytes.toBytes("col1"));
        Assert.assertEquals("Matching value 1", "attr1_val" + i,
            Bytes.toString(v1));
        byte[] v2 = r.getValue(Bytes.toBytes(tableFamily2),
            Bytes.toBytes("col2"));
        Assert.assertEquals("Matching value 2", "attr2_val" + i,
            Bytes.toString(v2));
      }
    } finally {
      table.close();
    }
  }

  /**
   * Write events to a sink directly, verify by scanning HBase table. x
   */
  @Test
  public void testSinkEmptyCol() throws IOException, InterruptedException {
    final String tableName = "testSinkEmptyCol";
    final String tableFamily1 = "family1";
    final String tableFamily2 = "family2";

    // create the table and column family to be used by sink
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(tableFamily1));
    desc.addFamily(new HColumnDescriptor(tableFamily2));
    HBaseAdmin admin = new HBaseAdmin(hbaseEnv.conf);
    admin.createTable(desc);

    // explicit constructor rather than builder - we want to control the conf
    List<QualifierSpec> spec = new ArrayList<QualifierSpec>();
    spec.add(new QualifierSpec(tableFamily1, "", "%{attr1}"));
    spec.add(new QualifierSpec(tableFamily2, "", "%{attr2}"));
    HBaseSink snk = new HBaseSink(tableName, "%{rowkey}", spec, 0L, false,
        hbaseEnv.conf);
    shipThreeEvents(snk);

    // verify that the events made it into hbase
    HTable table = new HTable(hbaseEnv.conf, tableName);
    try {
      for (long i = 0; i <= 2; i++) {
        Result r = table.get(new Get(Bytes.toBytes("row-key" + i)));
        LOG.info("result " + r);

        byte[] v1 = r.getValue(Bytes.toBytes(tableFamily1), Bytes.toBytes(""));
        Assert.assertEquals("Matching value 1", "attr1_val" + i,
            Bytes.toString(v1));
        byte[] v2 = r.getValue(Bytes.toBytes(tableFamily2), Bytes.toBytes(""));
        Assert.assertEquals("Matching value 2", "attr2_val" + i,
            Bytes.toString(v2));
      }
    } finally {
      table.close();
    }
  }

  /**
   * Write events to a sink directly, verify by scanning HBase table. x
   */
  @Test
  public void testSinkEscaping() throws IOException, InterruptedException {
    final String tableName = "testSinkEscaping";
    final String tableFamily1 = "family1";
    final String tableFamily2 = "family2";

    // create the table and column family to be used by sink
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(tableFamily1));
    desc.addFamily(new HColumnDescriptor(tableFamily2));
    HBaseAdmin admin = new HBaseAdmin(hbaseEnv.conf);
    admin.createTable(desc);

    // explicit constructor rather than builder - we want to control the conf
    List<QualifierSpec> spec = new ArrayList<QualifierSpec>();
    spec.add(new QualifierSpec(tableFamily1, "%{priority}", "%{body}"));
    spec.add(new QualifierSpec(tableFamily2, "col2", "%{badescape}"));
    HBaseSink snk = new HBaseSink(tableName, "%{host}-%{rowkey}", spec, 0L,
        false, hbaseEnv.conf);
    shipThreeEvents(snk);

    // verify that the events made it into hbase
    HTable table = new HTable(hbaseEnv.conf, tableName);
    try {
      for (long i = 0; i <= 2; i++) {
        Result r = table.get(new Get(Bytes.toBytes(DEFAULT_HOST + "-row-key"
            + i)));
        LOG.info("result " + r);

        byte[] v1 = r.getValue(Bytes.toBytes(tableFamily1),
            Bytes.toBytes(Event.Priority.INFO.toString())); // default prio
        Assert
            .assertEquals("Matching val 1", "message" + i, Bytes.toString(v1));
        byte[] v2 = r.getValue(Bytes.toBytes(tableFamily2),
            Bytes.toBytes("col2"));
        Assert.assertEquals("Matching val 2", "", Bytes.toString(v2));
      }
    } finally {
      table.close();
    }
  }

  @Test(expected = TableNotFoundException.class)
  public void testOpenFailBadTable() throws IOException {
    final String tableFamily1 = "family1";
    final String tableFamily2 = "family2";

    // explicit constructor rather than builder - we want to control the conf
    List<QualifierSpec> spec = new ArrayList<QualifierSpec>();
    spec.add(new QualifierSpec(tableFamily1, "col1", "%{attr1}"));
    spec.add(new QualifierSpec(tableFamily2, "col2", "%{attr2}")); // invalid
    HBaseSink snk = new HBaseSink("bogus table name", "%{rowkey}", spec, 0L,
        false, hbaseEnv.conf);
    shipThreeEvents(snk);
  }

  @Test(expected = IOException.class)
  public void testOpenFailBadColFam() throws IOException {
    final String tableName = "testSinkFailBadColFamf";
    final String tableFamily1 = "family1";
    final String tableFamily2 = "family2";

    // create the table and column family to be used by sink
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(tableFamily1));
    HBaseAdmin admin = new HBaseAdmin(hbaseEnv.conf);
    admin.createTable(desc);

    // explicit constructor rather than builder - we want to control the conf
    List<QualifierSpec> spec = new ArrayList<QualifierSpec>();
    spec.add(new QualifierSpec(tableFamily1, "col1", "%{attr1}"));
    spec.add(new QualifierSpec(tableFamily2, "col2", "%{attr2}")); // invalid
    HBaseSink snk = new HBaseSink(tableName, "%{rowkey}", spec, 0L, false,
        hbaseEnv.conf);
    shipThreeEvents(snk);
  }

}
