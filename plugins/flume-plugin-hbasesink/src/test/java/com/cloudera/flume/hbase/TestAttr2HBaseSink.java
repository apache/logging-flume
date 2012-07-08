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

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.Event.Priority;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.Clock;
import junit.framework.Assert;
import org.apache.hadoop.hbase.HBaseTestCase;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.AbstractMap;

/**
 * Test the hbase sink writes events to a table/family properly
 */
public class TestAttr2HBaseSink {
  private static HBaseTestEnv hbaseEnv;

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

  /**
   * Write events to a sink directly, verify by scanning HBase table.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testSink() throws IOException, InterruptedException {
    final String tableName = "testSink";
    final String tableSysFamily = "sysFamily";
    final String tableFamily1 = "family1";
    final String tableFamily2 = "family2";
    final String tableBody = "sysFamily";
    final String tableBodyFamily = "event";
    final Boolean writeBody = true;

    // create the table and column family to be used by sink
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(tableSysFamily));
    desc.addFamily(new HColumnDescriptor(tableFamily1));
    desc.addFamily(new HColumnDescriptor(tableFamily2));
    HBaseAdmin admin = new HBaseAdmin(hbaseEnv.conf);
    admin.createTable(desc);

    // explicit constructor rather than builder - we want to control the conf
    EventSink snk = new Attr2HBaseEventSink(tableName, tableSysFamily,
        writeBody, tableBody, tableBodyFamily, "2hb_", 0, true, hbaseEnv.conf);
    snk.open();
    try {
      Event e1 = new EventImpl("message0".getBytes(), Clock.unixTime(),
          Priority.INFO, 0, "localhost");
      e1.set("2hb_", Bytes.toBytes("row-key0"));
      e1.set("2hb_family1:column1", Bytes.toBytes("value0"));
      e1.set("2hb_family2:column2", Bytes.toBytes("value_0"));
      e1.set("other", Bytes.toBytes("val0"));
      Event e2 = new EventImpl("message1".getBytes(), Clock.unixTime(),
          Priority.INFO, 1, "localhost");
      e2.set("2hb_", Bytes.toBytes("row-key1"));
      e2.set("2hb_family1:column1", Bytes.toBytes("value1"));
      e2.set("2hb_family2:column2", Bytes.toBytes("value_1"));
      e2.set("other", Bytes.toBytes("val1"));
      Event e3 = new EventImpl("message2".getBytes(), Clock.unixTime(),
          Priority.INFO, 2, "localhost");
      e3.set("2hb_", Bytes.toBytes("row-key2"));
      e3.set("2hb_family1:column1", Bytes.toBytes("value2"));
      e3.set("2hb_family2:column2", Bytes.toBytes("value_2"));
      e3.set("other", Bytes.toBytes("val2"));
      snk.append(e1);
      snk.append(e2);
      snk.append(e3);
    } finally {
      snk.close();
    }

    // verify that the events made it into hbase
    HTable table = new HTable(hbaseEnv.conf, tableName);
    try {
      for (long i = 0; i <= 2; i++) {
        Result r = table.get(new Get(Bytes.toBytes("row-key" + i)));
        System.out.println("result " + r);

        byte[] host = r.getValue(Bytes.toBytes(tableSysFamily),
            Bytes.toBytes("host"));
        Assert.assertEquals("Matching host", "localhost", Bytes.toString(host));
        byte[] priority = r.getValue(Bytes.toBytes(tableSysFamily),
            Bytes.toBytes("priority"));
        Assert.assertEquals("Matching priority", "INFO",
            Bytes.toString(priority));
        byte[] body = r.getValue(Bytes.toBytes(tableSysFamily),
            Bytes.toBytes("event"));
        Assert.assertEquals("Matching body", "message" + i,
            Bytes.toString(body));
        // 4 here means: host, timestamp, priority and body
        Assert.assertEquals("Matching values added", 4,
            r.getFamilyMap(Bytes.toBytes(tableSysFamily)).size());

        byte[] fam1value = r.getValue(Bytes.toBytes(tableFamily1),
            Bytes.toBytes("column1"));
        Assert.assertEquals("Matching value", "value" + i,
            Bytes.toString(fam1value));
        Assert.assertEquals("Matching values added", 1,
            r.getFamilyMap(Bytes.toBytes(tableFamily1)).size());

        byte[] fam2value = r.getValue(Bytes.toBytes(tableFamily2),
            Bytes.toBytes("column2"));
        Assert.assertEquals("Matching value", "value_" + i,
            Bytes.toString(fam2value));
        Assert.assertEquals("Matching values added", 1,
            r.getFamilyMap(Bytes.toBytes(tableFamily2)).size());

      }
    } finally {
      table.close();
    }
  }

  @Test
  public void testCreatePutWithoutSystemColumnFamily() {
    final String tableBody = "sysFamily";
    final String tableBodyFamily = "event";
    final Boolean writeBody = true;
    Attr2HBaseEventSink snk = new Attr2HBaseEventSink("tableName", "",
        writeBody, tableBody, tableBodyFamily, "2hb_", 0, true, null);

    Event e = new EventImpl("message".getBytes(), Clock.unixTime(),
        Priority.INFO, 0, "localhost");
    e.set("2hb_", Bytes.toBytes("rowKey"));
    e.set("2hb_family1:column1", Bytes.toBytes("value1"));
    e.set("2hb_family2:column2", Bytes.toBytes("value2"));
    e.set("other", Bytes.toBytes("val"));

    Put put = snk.createPut(e);
    Assert.assertEquals("Matching row key", "rowKey",
        Bytes.toString(put.getRow()));
    Assert.assertEquals("Matching column families added", 2, put.getFamilyMap()
        .size());
    Assert.assertEquals(
        "Matching value",
        "value1",
        Bytes.toString(put
            .get(Bytes.toBytes("family1"), Bytes.toBytes("column1")).get(0)
            .getValue()));
    Assert.assertEquals(
        "Matching value",
        "value2",
        Bytes.toString(put
            .get(Bytes.toBytes("family2"), Bytes.toBytes("column2")).get(0)
            .getValue()));
  }

  @Test
  public void testDontWriteBody() {
    final String tableBody = "";
    final String tableBodyFamily = "";
    final Boolean writeBody = false;
    Attr2HBaseEventSink snk = new Attr2HBaseEventSink("tableName", "sysFam",
        writeBody, tableBody, tableBodyFamily, "2hb_", 0, true, null);

    Event e = new EventImpl("message".getBytes(), Clock.unixTime(),
        Priority.INFO, 0, "localhost");
    e.set("2hb_", Bytes.toBytes("rowKey"));

    Put put = snk.createPut(e);
    // 3 here means: host, timestamp, priority (body is excluded)
    Assert.assertEquals("Matching values added", 3,
        put.getFamilyMap().get(Bytes.toBytes("sysFam")).size());
    Assert.assertEquals("Body shouldn't be written", 0,
        put.get(Bytes.toBytes("sysFam"), Bytes.toBytes("event")).size());
  }

  @Test
  public void testCreatePutWithoutExplicitRowKey() {
    final String tableBody = "sysFamily";
    final String tableBodyFamily = "event";
    final Boolean writeBody = true;
    Attr2HBaseEventSink snk = new Attr2HBaseEventSink("tableName", "sysFam",
        writeBody, tableBody, tableBodyFamily, "2hb_", 0, true, null);

    Event e = new EventImpl("message".getBytes(), Clock.unixTime(),
        Priority.INFO, 0, "localhost");
    e.set("other", Bytes.toBytes("val0"));

    Put put = snk.createPut(e);
    Assert.assertNull("No put should be created when row key data is absent",
        put);
  }

  @Test
  public void testAddAttributeWithSystemColumnFamSpecified() {
    final String tableBody = "sysFamily";
    final String tableBodyFamily = "event";
    final Boolean writeBody = true;
    Attr2HBaseEventSink snk = new Attr2HBaseEventSink("tableName", "sysFam",
        writeBody, tableBody, tableBodyFamily, "2hb_", 0, true, null);
    byte[] foo = Bytes.toBytes("foo");

    Put put = new Put(foo);
    snk.attemptToAddAttribute(put, createAttr(":any:", foo));
    // since attribute has no needed prefix, it shouldn't be added
    assertEmpty(put);

    put = new Put(foo);
    snk.attemptToAddAttribute(put, createAttr("2hb_:any:", foo));
    // since attribute has prefix, but isn't contain colFam *and* qualifier, it
    // should be put into "system" colFam
    assertHasSingleKeyValue(put, "sysFam", ":any:", foo);

    put = new Put(foo);
    snk.attemptToAddAttribute(put, createAttr("2hb_columnFam:columnName", foo));
    assertHasSingleKeyValue(put, "columnFam", "columnName", foo);
  }

  @Test
  public void testAddAttributeWithoutSystemColumnFam() {
    final String tableBody = "sysFamily";
    final String tableBodyFamily = "event";
    final Boolean writeBody = true;
    Attr2HBaseEventSink snk = new Attr2HBaseEventSink("tableName", "",
        writeBody, tableBody, tableBodyFamily, "2hb_", 0, true, null);
    byte[] foo = Bytes.toBytes("foo");

    Put put = new Put(foo);
    // since attribute has no needed prefix, it shouldn't be added
    snk.attemptToAddAttribute(put, createAttr(":any:", foo));
    assertEmpty(put);

    put = new Put(foo);
    // since attribute has prefix, but isn't contain colFam *and* qualifier and
    // "system" colFam isn't specified it should be omitted
    snk.attemptToAddAttribute(put, createAttr("2hb_:any:", foo));
    assertEmpty(put);

    put = new Put(foo);
    snk.attemptToAddAttribute(put, createAttr("2hb_columnFam:columnName", foo));
    assertHasSingleKeyValue(put, "columnFam", "columnName", foo);
  }

  private void assertHasSingleKeyValue(Put put, String fam, String col,
      byte[] val) {
    Assert.assertEquals("Matching added values", 1, put.getFamilyMap().size());
    Assert.assertTrue(
        "Matching value",
        Bytes.compareTo(val, put.get(Bytes.toBytes(fam), Bytes.toBytes(col))
            .get(0).getValue()) == 0);
  }

  private void assertEmpty(Put put) {
    Assert.assertEquals("Matching added values", 0, put.getFamilyMap().size());
  }

  private AbstractMap.SimpleEntry<String, byte[]> createAttr(String key,
      byte[] val) {
    return new AbstractMap.SimpleEntry<String, byte[]>(key, val);
  }

  @Test
  public void testAddAttributeWithSystemColumnFamSpecifiedAndEmptyPrefix() {
    final String tableBody = "sysFamily";
    final String tableBodyFamily = "event";
    final Boolean writeBody = true;
    Attr2HBaseEventSink snk = new Attr2HBaseEventSink("tableName", "sysFam",
        writeBody, tableBody, tableBodyFamily, "", 0, true, null);
    byte[] foo = Bytes.toBytes("foo");

    Put put = new Put(foo);
    snk.attemptToAddAttribute(put, createAttr(":any:", foo));
    assertHasSingleKeyValue(put, "sysFam", ":any:", foo);

    put = new Put(foo);
    snk.attemptToAddAttribute(put, createAttr("columnFam:columnName", foo));
    assertHasSingleKeyValue(put, "columnFam", "columnName", foo);
  }
}
