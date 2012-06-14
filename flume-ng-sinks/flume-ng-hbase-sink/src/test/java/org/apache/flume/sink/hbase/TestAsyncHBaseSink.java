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


import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.primitives.Longs;

public class TestAsyncHBaseSink {
  private static HBaseTestingUtility testUtility;
  private static MiniZooKeeperCluster zookeeperCluster;
  private static MiniHBaseCluster hbaseCluster;
  private static String workDir = "./testFlumeHbaseSink";

  private static String tableName = "TestHbaseSink";
  private static String columnFamily = "TestColumnFamily";
  private static String inColumn = "Increment";
  private static String plCol = "pc";
  private static Context ctx = new Context();
  private static String valBase = "testing hbase sink: jham";


  @BeforeClass
  public static void setUp() throws Exception {

    /*
     * Borrowed from HCatalog ManyMiniCluster.java
     * https://svn.apache.org/repos/asf/incubator/hcatalog/trunk/
     * storage-handlers/hbase/src/test/org/apache/hcatalog/
     * hbase/ManyMiniCluster.java
     *
     */
    String hbaseDir = new File(workDir,"hbase").getAbsolutePath();
    String hbaseRoot = "file://" + hbaseDir;
    Configuration hbaseConf =  HBaseConfiguration.create();

    hbaseConf.set(HConstants.HBASE_DIR, hbaseRoot);
    hbaseConf.set("hbase.master", "local");
    hbaseConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "0.0.0.0");
    hbaseConf.setInt("hbase.master.info.port", -1);
    hbaseConf.setInt("hbase.zookeeper.property.maxClientCnxns",500);
    String zookeeperDir = new File(workDir,"zk").getAbsolutePath();
    int zookeeperPort = 2181;
    zookeeperCluster = new MiniZooKeeperCluster();
    zookeeperCluster.setDefaultClientPort(zookeeperPort);
    zookeeperCluster.startup(new File(zookeeperDir));
    hbaseCluster= new MiniHBaseCluster(hbaseConf, 1);
    hbaseConf.set("hbase.master",
        hbaseCluster.getMaster().getServerName().getHostAndPort());
    testUtility = new HBaseTestingUtility(hbaseConf);
    testUtility.setZkCluster(zookeeperCluster);
    hbaseCluster.startMaster();
    testUtility.createTable(tableName.getBytes(), columnFamily.getBytes());
    Map<String, String> ctxMap = new HashMap<String, String>();
    ctxMap.put("table", tableName);
    ctxMap.put("columnFamily", columnFamily);
    ctxMap.put("serializer",
        "org.apache.flume.sink.hbase.SimpleAsyncHbaseEventSerializer");
    ctxMap.put("serializer.payloadColumn", plCol);
    ctxMap.put("serializer.incrementColumn", inColumn);
    ctx.putAll(ctxMap);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    zookeeperCluster.shutdown();
    hbaseCluster.shutdown();
    FileUtils.deleteDirectory(new File(workDir));
  }

  @Test
  public void testOneEvent() throws Exception {
    AsyncHBaseSink sink = new AsyncHBaseSink(testUtility.getConfiguration());
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
