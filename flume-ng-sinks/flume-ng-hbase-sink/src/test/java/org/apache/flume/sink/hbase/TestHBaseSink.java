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
package org.apache.flume.sink.hbase;


import java.util.HashMap;
import java.util.Map;


import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.sink.hbase.HBaseSink;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Assert;

import com.google.common.primitives.Longs;

public class TestHBaseSink {
  private static HBaseTestingUtility testUtility = new HBaseTestingUtility();
  private static String tableName = "TestHbaseSink";
  private static String columnFamily = "TestColumnFamily";
  private static String inColumn = "Increment";
  private static String plCol = "pc";
  private static Context ctx = new Context();
  private static String valBase = "testing hbase sink: jham";


  @BeforeClass
  public static void setUp() throws Exception {
    testUtility.startMiniCluster();
    testUtility.createTable(tableName.getBytes(), columnFamily.getBytes());
    Map<String, String> ctxMap = new HashMap<String, String>();
    ctxMap.put("table", tableName);
    ctxMap.put("columnFamily", columnFamily);
    ctxMap.put("serializer",
        "org.apache.flume.sink.hbase.SimpleHbaseEventSerializer");
    ctxMap.put("serializer.payloadColumn", plCol);
    ctxMap.put("serializer.incrementColumn", inColumn);
    ctx.putAll(ctxMap);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    testUtility.shutdownMiniCluster();
  }

  @Test
  public void testOneEvent() throws Exception {
    HBaseSink sink = new HBaseSink(testUtility.getConfiguration());
    Configurables.configure(sink, ctx);
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    sink.setChannel(channel);
    sink.start();
    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e = EventBuilder.withBody(
        Bytes.toBytes(valBase));
    channel.put(e);
    tx.commit();
    tx.close();

    sink.process();
    sink.stop();
    HTable table = new HTable(testUtility.getConfiguration(), tableName);
    Scan scan = new Scan();
    scan.addColumn(columnFamily.getBytes(),plCol.getBytes());
    scan.setStartRow( Bytes.toBytes("default"));
    ResultScanner rs = table.getScanner(scan);
    byte[] out = null;
    try {
      for (Result r = rs.next(); r != null; r = rs.next()) {
        out = r.getValue(columnFamily.getBytes(), plCol.getBytes());
      }
    } finally {
      rs.close();
    }
    Assert.assertArrayEquals(e.getBody(), out);
    scan = new Scan();
    scan.addColumn(columnFamily.getBytes(),inColumn.getBytes());
    scan.setStartRow(Bytes.toBytes("incRow"));
    rs = table.getScanner(scan);
    out = null;
    try {
      for (Result r = rs.next(); r != null; r = rs.next()) {
        out = r.getValue(columnFamily.getBytes(), inColumn.getBytes());
      }
    } finally {
      rs.close();
    }
    System.out.println(out);
    Assert.assertArrayEquals(Longs.toByteArray(1), out);
  }
}

