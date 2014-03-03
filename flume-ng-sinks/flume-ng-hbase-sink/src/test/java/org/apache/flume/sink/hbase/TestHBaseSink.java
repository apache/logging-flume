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

import static org.mockito.Mockito.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Longs;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHBaseSink {
  private static final Logger logger =
      LoggerFactory.getLogger(TestHBaseSink.class);

  private static final HBaseTestingUtility testUtility = new HBaseTestingUtility();
  private static final String tableName = "TestHbaseSink";
  private static final String columnFamily = "TestColumnFamily";
  private static final String inColumn = "iCol";
  private static final String plCol = "pCol";
  private static final String valBase = "testing hbase sink: jham";

  private Configuration conf;
  private Context ctx;

  @BeforeClass
  public static void setUpOnce() throws Exception {
    testUtility.startMiniCluster();
  }

  @AfterClass
  public static void tearDownOnce() throws Exception {
    testUtility.shutdownMiniCluster();
  }

  /**
   * Most common context setup for unit tests using
   * {@link SimpleHbaseEventSerializer}.
   */
  @Before
  public void setUp() throws IOException {
    conf = new Configuration(testUtility.getConfiguration());
    ctx = new Context();
    testUtility.createTable(tableName.getBytes(), columnFamily.getBytes());
  }

  @After
  public void tearDown() throws IOException {
    testUtility.deleteTable(tableName.getBytes());
  }

  /**
   * Set up {@link Context} for use with {@link SimpleHbaseEventSerializer}.
   */
  private void initContextForSimpleHbaseEventSerializer() {
    ctx = new Context();
    ctx.put("table", tableName);
    ctx.put("columnFamily", columnFamily);
    ctx.put("serializer", SimpleHbaseEventSerializer.class.getName());
    ctx.put("serializer.payloadColumn", plCol);
    ctx.put("serializer.incrementColumn", inColumn);
  }

  /**
   * Set up {@link Context} for use with {@link IncrementHBaseSerializer}.
   */
  private void initContextForIncrementHBaseSerializer() {
    ctx = new Context();
    ctx.put("table", tableName);
    ctx.put("columnFamily", columnFamily);
    ctx.put("serializer", IncrementHBaseSerializer.class.getName());
  }

  @Test
  public void testOneEventWithDefaults() throws Exception {
    //Create a context without setting increment column and payload Column
    ctx = new Context();
    ctx.put("table", tableName);
    ctx.put("columnFamily", columnFamily);
    ctx.put("serializer", SimpleHbaseEventSerializer.class.getName());

    HBaseSink sink = new HBaseSink(conf);
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
    HTable table = new HTable(conf, tableName);
    byte[][] results = getResults(table, 1);
    byte[] out = results[0];
    Assert.assertArrayEquals(e.getBody(), out);
    out = results[1];
    Assert.assertArrayEquals(Longs.toByteArray(1), out);
  }

  @Test
  public void testOneEvent() throws Exception {
    initContextForSimpleHbaseEventSerializer();
    HBaseSink sink = new HBaseSink(conf);
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
    HTable table = new HTable(conf, tableName);
    byte[][] results = getResults(table, 1);
    byte[] out = results[0];
    Assert.assertArrayEquals(e.getBody(), out);
    out = results[1];
    Assert.assertArrayEquals(Longs.toByteArray(1), out);
  }

  @Test
  public void testThreeEvents() throws Exception {
    initContextForSimpleHbaseEventSerializer();
    ctx.put("batchSize", "3");
    HBaseSink sink = new HBaseSink(conf);
    Configurables.configure(sink, ctx);
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
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
    sink.stop();
    HTable table = new HTable(conf, tableName);
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
  public void testMultipleBatches() throws Exception {
    initContextForSimpleHbaseEventSerializer();
    ctx.put("batchSize", "2");
    HBaseSink sink = new HBaseSink(conf);
    Configurables.configure(sink, ctx);
    //Reset the context to a higher batchSize
    ctx.put("batchSize", "100");
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
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
    while(sink.process() != Status.BACKOFF){
      count++;
    }
    sink.stop();
    Assert.assertEquals(2, count);
    HTable table = new HTable(conf, tableName);
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
    logger.info("Running testMissingTable()");
    initContextForSimpleHbaseEventSerializer();

    // setUp() will create the table, so we delete it.
    logger.info("Deleting table {}", tableName);
    testUtility.deleteTable(tableName.getBytes());

    ctx.put("batchSize", "2");
    HBaseSink sink = new HBaseSink(conf);
    Configurables.configure(sink, ctx);
    //Reset the context to a higher batchSize
    ctx.put("batchSize", "100");
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    sink.setChannel(channel);

    logger.info("Writing data into channel");
    Transaction tx = channel.getTransaction();
    tx.begin();
    for(int i = 0; i < 3; i++){
      Event e = EventBuilder.withBody(Bytes.toBytes(valBase + "-" + i));
      channel.put(e);
    }
    tx.commit();
    tx.close();

    logger.info("Starting sink and processing events");
    try {
      logger.info("Calling sink.start()");
      sink.start(); // This method will throw.

      // We never get here, but we log in case the behavior changes.
      logger.error("Unexpected error: Calling sink.process()");
      sink.process();
      logger.error("Unexpected error: Calling sink.stop()");
      sink.stop();
    } finally {
      // Re-create the table so tearDown() doesn't throw.
      testUtility.createTable(tableName.getBytes(), columnFamily.getBytes());
    }

    // FIXME: The test should never get here, the below code doesn't run.
    Assert.fail();

    HTable table = new HTable(conf, tableName);
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
  }

  // TODO: Move this test to a different class and run it stand-alone.
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
    initContextForSimpleHbaseEventSerializer();
    ctx.put("batchSize", "2");
    HBaseSink sink = new HBaseSink(conf);
    Configurables.configure(sink, ctx);
    //Reset the context to a higher batchSize
    ctx.put("batchSize", "100");
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
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
    HTable table = new HTable(conf, tableName);
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
    testUtility.shutdownMiniCluster();
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

  @Test
  public void testTransactionStateOnChannelException() throws Exception {
    initContextForSimpleHbaseEventSerializer();
    ctx.put("batchSize", "1");

    HBaseSink sink = new HBaseSink(conf);
    Configurables.configure(sink, ctx);
    // Reset the context to a higher batchSize
    Channel channel = spy(new MemoryChannel());
    Configurables.configure(channel, new Context());
    sink.setChannel(channel);
    sink.start();
    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e = EventBuilder.withBody(Bytes.toBytes(valBase + "-" + 0));
    channel.put(e);
    tx.commit();
    tx.close();
    doThrow(new ChannelException("Mock Exception")).when(channel).take();
    try {
      sink.process();
      Assert.fail("take() method should throw exception");
    } catch (ChannelException ex) {
      Assert.assertEquals("Mock Exception", ex.getMessage());
    }
    doReturn(e).when(channel).take();
    sink.process();
    sink.stop();
    HTable table = new HTable(conf, tableName);
    byte[][] results = getResults(table, 1);
    byte[] out = results[0];
    Assert.assertArrayEquals(e.getBody(), out);
    out = results[1];
    Assert.assertArrayEquals(Longs.toByteArray(1), out);
  }

  @Test
  public void testTransactionStateOnSerializationException() throws Exception {
    initContextForSimpleHbaseEventSerializer();
    ctx.put("batchSize", "1");
    ctx.put(HBaseSinkConfigurationConstants.CONFIG_SERIALIZER,
        "org.apache.flume.sink.hbase.MockSimpleHbaseEventSerializer");

    HBaseSink sink = new HBaseSink(conf);
    Configurables.configure(sink, ctx);
    // Reset the context to a higher batchSize
    ctx.put("batchSize", "100");
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    sink.setChannel(channel);
    sink.start();
    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e = EventBuilder.withBody(Bytes.toBytes(valBase + "-" + 0));
    channel.put(e);
    tx.commit();
    tx.close();
    try {
      MockSimpleHbaseEventSerializer.throwException = true;
      sink.process();
      Assert.fail("FlumeException expected from serilazer");
    } catch (FlumeException ex) {
      Assert.assertEquals("Exception for testing", ex.getMessage());
    }
    MockSimpleHbaseEventSerializer.throwException = false;
    sink.process();
    sink.stop();
    HTable table = new HTable(conf, tableName);
    byte[][] results = getResults(table, 1);
    byte[] out = results[0];
    Assert.assertArrayEquals(e.getBody(), out);
    out = results[1];
    Assert.assertArrayEquals(Longs.toByteArray(1), out);
  }

  @Test
  public void testWithoutConfigurationObject() throws Exception{
    initContextForSimpleHbaseEventSerializer();
    Context tmpContext = new Context(ctx.getParameters());
    tmpContext.put("batchSize", "2");
    tmpContext.put(HBaseSinkConfigurationConstants.ZK_QUORUM,
      ZKConfig.getZKQuorumServersString(conf) );
    System.out.print(ctx.getString(HBaseSinkConfigurationConstants.ZK_QUORUM));
    tmpContext.put(HBaseSinkConfigurationConstants.ZK_ZNODE_PARENT,
      conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT));

    HBaseSink sink = new HBaseSink();
    Configurables.configure(sink, tmpContext);
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
    Status status = Status.READY;
    while(status != Status.BACKOFF){
      status = sink.process();
    }
    sink.stop();
    HTable table = new HTable(conf, tableName);
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
  public void testZKQuorum() throws Exception{
    initContextForSimpleHbaseEventSerializer();
    Context tmpContext = new Context(ctx.getParameters());
    String zkQuorum = "zk1.flume.apache.org:3342, zk2.flume.apache.org:3342, " +
      "zk3.flume.apache.org:3342";
    tmpContext.put("batchSize", "2");
    tmpContext.put(HBaseSinkConfigurationConstants.ZK_QUORUM, zkQuorum);
    tmpContext.put(HBaseSinkConfigurationConstants.ZK_ZNODE_PARENT,
      conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT));
    HBaseSink sink = new HBaseSink();
    Configurables.configure(sink, tmpContext);
    Assert.assertEquals("zk1.flume.apache.org,zk2.flume.apache.org," +
      "zk3.flume.apache.org", sink.getConfig().get(HConstants
      .ZOOKEEPER_QUORUM));
    Assert.assertEquals(String.valueOf(3342), sink.getConfig().get(HConstants
      .ZOOKEEPER_CLIENT_PORT));
  }

  @Test (expected = FlumeException.class)
  public void testZKQuorumIncorrectPorts() throws Exception{
    initContextForSimpleHbaseEventSerializer();
    Context tmpContext = new Context(ctx.getParameters());

    String zkQuorum = "zk1.flume.apache.org:3345, zk2.flume.apache.org:3342, " +
      "zk3.flume.apache.org:3342";
    tmpContext.put("batchSize", "2");
    tmpContext.put(HBaseSinkConfigurationConstants.ZK_QUORUM, zkQuorum);
    tmpContext.put(HBaseSinkConfigurationConstants.ZK_ZNODE_PARENT,
      conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT));
    HBaseSink sink = new HBaseSink();
    Configurables.configure(sink, tmpContext);
    Assert.fail();
  }

  @Test
  public void testCoalesce() throws EventDeliveryException {
    initContextForIncrementHBaseSerializer();
    ctx.put("batchSize", "100");
    ctx.put(HBaseSinkConfigurationConstants.CONFIG_COALESCE_INCREMENTS,
        String.valueOf(true));

    final Map<String, Long> expectedCounts = Maps.newHashMap();
    expectedCounts.put("r1:c1", 10L);
    expectedCounts.put("r1:c2", 20L);
    expectedCounts.put("r2:c1", 7L);
    expectedCounts.put("r2:c3", 63L);
    HBaseSink.DebugIncrementsCallback cb = new CoalesceValidator(expectedCounts);

    HBaseSink sink = new HBaseSink(testUtility.getConfiguration(), cb);
    Configurables.configure(sink, ctx);
    Channel channel = createAndConfigureMemoryChannel(sink);

    List<Event> events = Lists.newLinkedList();
    generateEvents(events, expectedCounts);
    putEvents(channel, events);

    sink.start();
    sink.process(); // Calls CoalesceValidator instance.
    sink.stop();
  }

  @Test(expected = AssertionError.class)
  public void negativeTestCoalesce() throws EventDeliveryException {
    initContextForIncrementHBaseSerializer();
    ctx.put("batchSize", "10");

    final Map<String, Long> expectedCounts = Maps.newHashMap();
    expectedCounts.put("r1:c1", 10L);
    HBaseSink.DebugIncrementsCallback cb = new CoalesceValidator(expectedCounts);

    HBaseSink sink = new HBaseSink(testUtility.getConfiguration(), cb);
    Configurables.configure(sink, ctx);
    Channel channel = createAndConfigureMemoryChannel(sink);

    List<Event> events = Lists.newLinkedList();
    generateEvents(events, expectedCounts);
    putEvents(channel, events);

    sink.start();
    sink.process(); // Calls CoalesceValidator instance.
    sink.stop();
  }

  @Test
  public void testBatchAware() throws EventDeliveryException {
    logger.info("Running testBatchAware()");
    initContextForIncrementHBaseSerializer();
    HBaseSink sink = new HBaseSink(testUtility.getConfiguration());
    Configurables.configure(sink, ctx);
    Channel channel = createAndConfigureMemoryChannel(sink);

    sink.start();
    int batchCount = 3;
    for (int i = 0; i < batchCount; i++) {
      sink.process();
    }
    sink.stop();
    Assert.assertEquals(batchCount,
        ((IncrementHBaseSerializer) sink.getSerializer()).getNumBatchesStarted());
  }

  /**
   * For testing that the rows coalesced, serialized by
   * {@link IncrementHBaseSerializer}, are of the expected batch size.
   */
  private static class CoalesceValidator
      implements HBaseSink.DebugIncrementsCallback {

    private final Map<String,Long> expectedCounts;
    private final Method refGetFamilyMap;

    public CoalesceValidator(Map<String, Long> expectedCounts) {
      this.expectedCounts = expectedCounts;
      this.refGetFamilyMap = HBaseSink.reflectLookupGetFamilyMap();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void onAfterCoalesce(Iterable<Increment> increments) {
      for (Increment inc : increments) {
        byte[] row = inc.getRow();
        Map<byte[], NavigableMap<byte[], Long>> families = null;
        try {
          families = (Map<byte[], NavigableMap<byte[], Long>>)
              refGetFamilyMap.invoke(inc);
        } catch (Exception e) {
          Throwables.propagate(e);
        }
        for (byte[] family : families.keySet()) {
          NavigableMap<byte[], Long> qualifiers = families.get(family);
          for (Map.Entry<byte[], Long> entry : qualifiers.entrySet()) {
            byte[] qualifier = entry.getKey();
            Long count = entry.getValue();
            StringBuilder b = new StringBuilder(20);
            b.append(new String(row, Charsets.UTF_8));
            b.append(':');
            b.append(new String(qualifier, Charsets.UTF_8));
            String key = b.toString();
            Assert.assertEquals("Expected counts don't match observed for " + key,
                expectedCounts.get(key), count);
          }
        }
      }
    }
  }

  /**
   * Add number of Events corresponding to counts to the events list.
   * @param events Destination list.
   * @param counts How many events to generate for each row:qualifier pair.
   */
  private void generateEvents(List<Event> events, Map<String, Long> counts) {
    for (String key : counts.keySet()) {
      long count = counts.get(key);
      for (long i = 0; i < count; i++) {
        events.add(EventBuilder.withBody(key, Charsets.UTF_8));
      }
    }
  }

  private Channel createAndConfigureMemoryChannel(HBaseSink sink) {
    Channel channel = new MemoryChannel();
    Context channelCtx = new Context();
    channelCtx.put("capacity", String.valueOf(1000L));
    channelCtx.put("transactionCapacity", String.valueOf(1000L));
    Configurables.configure(channel, channelCtx);
    sink.setChannel(channel);
    channel.start();
    return channel;
  }

  private void putEvents(Channel channel, Iterable<Event> events) {
    Transaction tx = channel.getTransaction();
    tx.begin();
    for (Event event : events) {
      channel.put(event);
    }
    tx.commit();
    tx.close();
  }

}
