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
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.Sink.Status;
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
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.io.Files;
import com.google.common.primitives.Longs;
import java.lang.reflect.Method;

import org.junit.After;

public class TestAsyncHBaseSink {
  private static HBaseTestingUtility testUtility;
  private static MiniZooKeeperCluster zookeeperCluster;
  private static MiniHBaseCluster hbaseCluster;
  private static String workDir = Files.createTempDir().getAbsolutePath();

  private static String tableName = "TestHbaseSink";
  private static String columnFamily = "TestColumnFamily";
  private static String inColumn = "iCol";
  private static String plCol = "pCol";
  private static Context ctx = new Context();
  private static String valBase = "testing hbase sink: jham";
  private boolean deleteTable = true;


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
    hbaseConf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "0.0.0.0");
    hbaseConf.setInt("hbase.master.info.port", -1);
    hbaseConf.setInt("hbase.zookeeper.property.maxClientCnxns",500);
    String zookeeperDir = new File(workDir,"zk").getAbsolutePath();
    int zookeeperPort = 2181;
    zookeeperCluster = new MiniZooKeeperCluster();
    Method m;
    Class<?> zkParam[] = {Integer.TYPE};
    try{
      m = MiniZooKeeperCluster.class.getDeclaredMethod("setDefaultClientPort",
          zkParam);
    } catch (NoSuchMethodException e) {
      m = MiniZooKeeperCluster.class.getDeclaredMethod("setClientPort",
          zkParam);
    }

    m.invoke(zookeeperCluster, new Object[] {new Integer(zookeeperPort)});
    zookeeperCluster.startup(new File(zookeeperDir));
    hbaseCluster = new MiniHBaseCluster(hbaseConf, 1);
    HMaster master = hbaseCluster.getMaster();
    Object serverName = master.getServerName();
    String hostAndPort;
    if(serverName instanceof String) {
      System.out.println("Server name is string, using HServerAddress.");
      m = HMaster.class.getDeclaredMethod("getMasterAddress",
          new Class<?>[]{});
      Class<?> clazz = Class.forName("org.apache.hadoop.hbase.HServerAddress");
      /*
       * Call method to get server address
       */
      Object serverAddr = clazz.cast(m.invoke(master, new Object[]{}));
      //returns the address as hostname:port
      hostAndPort = serverAddr.toString();
    } else {
      System.out.println("ServerName is org.apache.hadoop.hbase.ServerName," +
          "using getHostAndPort()");
      Class<?> clazz = Class.forName("org.apache.hadoop.hbase.ServerName");
      m = clazz.getDeclaredMethod("getHostAndPort", new Class<?>[] {});
      hostAndPort = m.invoke(serverName, new Object[]{}).toString();
    }

    hbaseConf.set("hbase.master", hostAndPort);
    testUtility = new HBaseTestingUtility(hbaseConf);
    testUtility.setZkCluster(zookeeperCluster);
    hbaseCluster.startMaster();
    Map<String, String> ctxMap = new HashMap<String, String>();
    ctxMap.put("table", tableName);
    ctxMap.put("columnFamily", columnFamily);
    ctxMap.put("serializer",
        "org.apache.flume.sink.hbase.SimpleAsyncHbaseEventSerializer");
    ctxMap.put("serializer.payloadColumn", plCol);
    ctxMap.put("serializer.incrementColumn", inColumn);
    ctxMap.put("keep-alive", "0");
    ctxMap.put("timeout", "10000");
    ctx.putAll(ctxMap);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    hbaseCluster.shutdown();
    zookeeperCluster.shutdown();
    FileUtils.deleteDirectory(new File(workDir));
  }

  @After
  public void tearDownTest() throws Exception {
    if (deleteTable) {
      testUtility.deleteTable(tableName.getBytes());
    }
  }

  @Test
  public void testOneEventWithDefaults() throws Exception {
    Map<String,String> ctxMap = new HashMap<String,String>();
    ctxMap.put("table", tableName);
    ctxMap.put("columnFamily", columnFamily);
    ctxMap.put("serializer",
            "org.apache.flume.sink.hbase.SimpleAsyncHbaseEventSerializer");
    ctxMap.put("keep-alive", "0");
    ctxMap.put("timeout", "10000");
    Context tmpctx = new Context();
    tmpctx.putAll(ctxMap);

    testUtility.createTable(tableName.getBytes(), columnFamily.getBytes());
    deleteTable = true;
    AsyncHBaseSink sink = new AsyncHBaseSink(testUtility.getConfiguration());
    Configurables.configure(sink, tmpctx);
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, tmpctx);
    sink.setChannel(channel);
    sink.start();
    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e = EventBuilder.withBody(
            Bytes.toBytes(valBase));
    channel.put(e);
    tx.commit();
    tx.close();
    Assert.assertFalse(sink.isConfNull());
    sink.process();
    sink.stop();
    HTable table = new HTable(testUtility.getConfiguration(), tableName);
    byte[][] results = getResults(table, 1);
    byte[] out = results[0];
    Assert.assertArrayEquals(e.getBody(), out);
    out = results[1];
    Assert.assertArrayEquals(Longs.toByteArray(1), out);
  }

  @Test
  public void testOneEvent() throws Exception {
    testUtility.createTable(tableName.getBytes(), columnFamily.getBytes());
    deleteTable = true;
    AsyncHBaseSink sink = new AsyncHBaseSink(testUtility.getConfiguration());
    Configurables.configure(sink, ctx);
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, ctx);
    sink.setChannel(channel);
    sink.start();
    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e = EventBuilder.withBody(
        Bytes.toBytes(valBase));
    channel.put(e);
    tx.commit();
    tx.close();
    Assert.assertFalse(sink.isConfNull());
    sink.process();
    sink.stop();
    HTable table = new HTable(testUtility.getConfiguration(), tableName);
    byte[][] results = getResults(table, 1);
    byte[] out = results[0];
    Assert.assertArrayEquals(e.getBody(), out);
    out = results[1];
    Assert.assertArrayEquals(Longs.toByteArray(1), out);
  }

  @Test
  public void testThreeEvents() throws Exception {
    testUtility.createTable(tableName.getBytes(), columnFamily.getBytes());
    deleteTable = true;
    AsyncHBaseSink sink = new AsyncHBaseSink(testUtility.getConfiguration());
    Configurables.configure(sink, ctx);
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, ctx);
    sink.setChannel(channel);
    sink.start();
    Transaction tx = channel.getTransaction();
    tx.begin();
    for(int i = 0; i < 3; i++){
      Event e = EventBuilder.withBody(Bytes.toBytes(valBase + "-" + i));
      channel.put(e);
    }
    tx.commit();
    tx.close();
    Assert.assertFalse(sink.isConfNull());
    sink.process();
    sink.stop();
    HTable table = new HTable(testUtility.getConfiguration(), tableName);
    byte[][] results = getResults(table, 3);
    byte[] out;
    int found = 0;
    for(int i = 0; i < 3; i++){
      for(int j = 0; j < 3; j++){
        if(Arrays.equals(results[j],Bytes.toBytes(valBase + "-" + i))){
          found++;
          break;
        }
      }
    }
    Assert.assertEquals(3, found);
    out = results[3];
    Assert.assertArrayEquals(Longs.toByteArray(3), out);
  }

  //This will without FLUME-1842's timeout fix - but with FLUME-1842's testing
  //oriented changes to the callback classes and using single threaded executor
  //for tests.
  @Test (expected = EventDeliveryException.class)
  public void testTimeOut() throws Exception {
    testUtility.createTable(tableName.getBytes(), columnFamily.getBytes());
    deleteTable = true;
    AsyncHBaseSink sink = new AsyncHBaseSink(testUtility.getConfiguration(),
      true);
    Configurables.configure(sink, ctx);
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, ctx);
    sink.setChannel(channel);
    sink.start();
    Transaction tx = channel.getTransaction();
    tx.begin();
    for(int i = 0; i < 3; i++){
      Event e = EventBuilder.withBody(Bytes.toBytes(valBase + "-" + i));
      channel.put(e);
    }
    tx.commit();
    tx.close();
    Assert.assertFalse(sink.isConfNull());
    sink.process();
    Assert.fail();
  }

  @Test
  public void testMultipleBatches() throws Exception {
    testUtility.createTable(tableName.getBytes(), columnFamily.getBytes());
    deleteTable = true;
    ctx.put("batchSize", "2");
    AsyncHBaseSink sink = new AsyncHBaseSink(testUtility.getConfiguration());
    Configurables.configure(sink, ctx);
    //Reset the context to a higher batchSize
    ctx.put("batchSize", "100");
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, ctx);
    sink.setChannel(channel);
    sink.start();
    Transaction tx = channel.getTransaction();
    tx.begin();
    for(int i = 0; i < 3; i++){
      Event e = EventBuilder.withBody(Bytes.toBytes(valBase + "-" + i));
      channel.put(e);
    }
    tx.commit();
    tx.close();
    int count = 0;
    Status status = Status.READY;
    while(status != Status.BACKOFF){
      count++;
      status = sink.process();
    }
    Assert.assertFalse(sink.isConfNull());
    sink.stop();
    Assert.assertEquals(2, count);
    HTable table = new HTable(testUtility.getConfiguration(), tableName);
    byte[][] results = getResults(table, 3);
    byte[] out;
    int found = 0;
    for(int i = 0; i < 3; i++){
      for(int j = 0; j < 3; j++){
        if(Arrays.equals(results[j],Bytes.toBytes(valBase + "-" + i))){
          found++;
          break;
        }
      }
    }
    Assert.assertEquals(3, found);
    out = results[3];
    Assert.assertArrayEquals(Longs.toByteArray(3), out);
  }

  @Test
  public void testWithoutConfigurationObject() throws Exception{
    testUtility.createTable(tableName.getBytes(), columnFamily.getBytes());
    deleteTable = true;
    ctx.put("batchSize", "2");
    ctx.put(HBaseSinkConfigurationConstants.ZK_QUORUM,
        testUtility.getConfiguration().get(HConstants.ZOOKEEPER_QUORUM));
    ctx.put(HBaseSinkConfigurationConstants.ZK_ZNODE_PARENT,
      testUtility.getConfiguration().get(HConstants.ZOOKEEPER_ZNODE_PARENT));
    AsyncHBaseSink sink = new AsyncHBaseSink();
    Configurables.configure(sink, ctx);
    // Reset context to values usable by other tests.
    ctx.put(HBaseSinkConfigurationConstants.ZK_QUORUM, null);
    ctx.put(HBaseSinkConfigurationConstants.ZK_ZNODE_PARENT,null);
    ctx.put("batchSize", "100");
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, ctx);
    sink.setChannel(channel);
    sink.start();
    Transaction tx = channel.getTransaction();
    tx.begin();
    for(int i = 0; i < 3; i++){
      Event e = EventBuilder.withBody(Bytes.toBytes(valBase + "-" + i));
      channel.put(e);
    }
    tx.commit();
    tx.close();
    int count = 0;
    Status status = Status.READY;
    while(status != Status.BACKOFF){
      count++;
      status = sink.process();
    }
    /*
     * Make sure that the configuration was picked up from the context itself
     * and not from a configuration object which was created by the sink.
     */
    Assert.assertTrue(sink.isConfNull());
    sink.stop();
    Assert.assertEquals(2, count);
    HTable table = new HTable(testUtility.getConfiguration(), tableName);
    byte[][] results = getResults(table, 3);
    byte[] out;
    int found = 0;
    for(int i = 0; i < 3; i++){
      for(int j = 0; j < 3; j++){
        if(Arrays.equals(results[j],Bytes.toBytes(valBase + "-" + i))){
          found++;
          break;
        }
      }
    }
    Assert.assertEquals(3, found);
    out = results[3];
    Assert.assertArrayEquals(Longs.toByteArray(3), out);
  }

  @Test(expected = FlumeException.class)
  public void testMissingTable() throws Exception {
    deleteTable = false;
    ctx.put("batchSize", "2");
    AsyncHBaseSink sink = new AsyncHBaseSink(testUtility.getConfiguration());
    Configurables.configure(sink, ctx);
    //Reset the context to a higher batchSize
    ctx.put("batchSize", "100");
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, ctx);
    sink.setChannel(channel);
    sink.start();
    Transaction tx = channel.getTransaction();
    tx.begin();
    for(int i = 0; i < 3; i++){
      Event e = EventBuilder.withBody(Bytes.toBytes(valBase + "-" + i));
      channel.put(e);
    }
    tx.commit();
    tx.close();
    sink.process();
    Assert.assertFalse(sink.isConfNull());
    HTable table = new HTable(testUtility.getConfiguration(), tableName);
    byte[][] results = getResults(table, 2);
    byte[] out;
    int found = 0;
    for(int i = 0; i < 2; i++){
      for(int j = 0; j < 2; j++){
        if(Arrays.equals(results[j],Bytes.toBytes(valBase + "-" + i))){
          found++;
          break;
        }
      }
    }
    Assert.assertEquals(2, found);
    out = results[2];
    Assert.assertArrayEquals(Longs.toByteArray(2), out);
    sink.process();
    sink.stop();
  }
  /**
   * This test must run last - it shuts down the minicluster :D
   * @throws Exception
   */
  @Ignore("For dev builds only:" +
      "This test takes too long, and this has to be run after all other" +
      "tests, since it shuts down the minicluster. " +
      "Comment out all other tests" +
      "and uncomment this annotation to run this test.")
  @Test(expected = EventDeliveryException.class)
  public void testHBaseFailure() throws Exception {
    ctx.put("batchSize", "2");
    testUtility.createTable(tableName.getBytes(), columnFamily.getBytes());
    deleteTable = false;
    AsyncHBaseSink sink = new AsyncHBaseSink(testUtility.getConfiguration());
    Configurables.configure(sink, ctx);
    //Reset the context to a higher batchSize
    ctx.put("batchSize", "100");
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, ctx);
    sink.setChannel(channel);
    sink.start();
    Transaction tx = channel.getTransaction();
    tx.begin();
    for(int i = 0; i < 3; i++){
      Event e = EventBuilder.withBody(Bytes.toBytes(valBase + "-" + i));
      channel.put(e);
    }
    tx.commit();
    tx.close();
    sink.process();
    Assert.assertFalse(sink.isConfNull());
    HTable table = new HTable(testUtility.getConfiguration(), tableName);
    byte[][] results = getResults(table, 2);
    byte[] out;
    int found = 0;
    for(int i = 0; i < 2; i++){
      for(int j = 0; j < 2; j++){
        if(Arrays.equals(results[j],Bytes.toBytes(valBase + "-" + i))){
          found++;
          break;
        }
      }
    }
    Assert.assertEquals(2, found);
    out = results[2];
    Assert.assertArrayEquals(Longs.toByteArray(2), out);
    hbaseCluster.shutdown();
    sink.process();
    sink.stop();
  }
  /**
   * Makes Hbase scans to get rows in the payload column and increment column
   * in the table given. Expensive, so tread lightly.
   * Calling this function multiple times for the same result set is a bad
   * idea. Cache the result set once it is returned by this function.
   * @param table
   * @param numEvents Number of events inserted into the table
   * @return
   * @throws IOException
   */
  private byte[][] getResults(HTable table, int numEvents) throws IOException{
    byte[][] results = new byte[numEvents+1][];
    Scan scan = new Scan();
    scan.addColumn(columnFamily.getBytes(),plCol.getBytes());
    scan.setStartRow( Bytes.toBytes("default"));
    ResultScanner rs = table.getScanner(scan);
    byte[] out = null;
    int i = 0;
    try {
      for (Result r = rs.next(); r != null; r = rs.next()) {
        out = r.getValue(columnFamily.getBytes(), plCol.getBytes());

        if(i >= results.length - 1){
          rs.close();
          throw new FlumeException("More results than expected in the table." +
              "Expected = " + numEvents +". Found = " + i);
        }
        results[i++] = out;
        System.out.println(out);
      }
    } finally {
      rs.close();
    }

    Assert.assertEquals(i, results.length - 1);
    scan = new Scan();
    scan.addColumn(columnFamily.getBytes(),inColumn.getBytes());
    scan.setStartRow(Bytes.toBytes("incRow"));
    rs = table.getScanner(scan);
    out = null;
    try {
      for (Result r = rs.next(); r != null; r = rs.next()) {
        out = r.getValue(columnFamily.getBytes(), inColumn.getBytes());
        results[i++] = out;
        System.out.println(out);
      }
    } finally {
      rs.close();
    }
    return results;
  }
}

