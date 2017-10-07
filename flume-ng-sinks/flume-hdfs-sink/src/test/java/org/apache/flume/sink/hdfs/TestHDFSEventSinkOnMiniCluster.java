/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.flume.sink.hdfs;

import com.google.common.base.Charsets;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import com.google.common.base.Throwables;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.EventBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit tests that exercise HDFSEventSink on an actual instance of HDFS.
 * TODO: figure out how to unit-test Kerberos-secured HDFS.
 */
public class TestHDFSEventSinkOnMiniCluster {

  private static final Logger logger =
      LoggerFactory.getLogger(TestHDFSEventSinkOnMiniCluster.class);

  private static final boolean KEEP_DATA = false;
  private static final String DFS_DIR = "target/test/dfs";
  private static final String TEST_BUILD_DATA_KEY = "test.build.data";

  private static MiniDFSCluster cluster = null;
  private static String oldTestBuildDataProp = null;

  @BeforeClass
  public static void setupClass() throws IOException {
    // set up data dir for HDFS
    File dfsDir = new File(DFS_DIR);
    if (!dfsDir.isDirectory()) {
      dfsDir.mkdirs();
    }
    // save off system prop to restore later
    oldTestBuildDataProp = System.getProperty(TEST_BUILD_DATA_KEY);
    System.setProperty(TEST_BUILD_DATA_KEY, DFS_DIR);
  }

  private static String getNameNodeURL(MiniDFSCluster cluster) {
    int nnPort = cluster.getNameNode().getNameNodeAddress().getPort();
    return "hdfs://localhost:" + nnPort;
  }

  /**
   * This is a very basic test that writes one event to HDFS and reads it back.
   */
  @Test
  public void simpleHDFSTest() throws EventDeliveryException, IOException {
    cluster = new MiniDFSCluster(new Configuration(), 1, true, null);
    cluster.waitActive();

    String outputDir = "/flume/simpleHDFSTest";
    Path outputDirPath = new Path(outputDir);

    logger.info("Running test with output dir: {}", outputDir);

    FileSystem fs = cluster.getFileSystem();
    // ensure output directory is empty
    if (fs.exists(outputDirPath)) {
      fs.delete(outputDirPath, true);
    }

    String nnURL = getNameNodeURL(cluster);
    logger.info("Namenode address: {}", nnURL);

    Context chanCtx = new Context();
    MemoryChannel channel = new MemoryChannel();
    channel.setName("simpleHDFSTest-mem-chan");
    channel.configure(chanCtx);
    channel.start();

    Context sinkCtx = new Context();
    sinkCtx.put("hdfs.path", nnURL + outputDir);
    sinkCtx.put("hdfs.fileType", HDFSWriterFactory.DataStreamType);
    sinkCtx.put("hdfs.batchSize", Integer.toString(1));

    HDFSEventSink sink = new HDFSEventSink();
    sink.setName("simpleHDFSTest-hdfs-sink");
    sink.configure(sinkCtx);
    sink.setChannel(channel);
    sink.start();

    // create an event
    String EVENT_BODY = "yarg!";
    channel.getTransaction().begin();
    try {
      channel.put(EventBuilder.withBody(EVENT_BODY, Charsets.UTF_8));
      channel.getTransaction().commit();
    } finally {
      channel.getTransaction().close();
    }

    // store event to HDFS
    sink.process();

    // shut down flume
    sink.stop();
    channel.stop();

    // verify that it's in HDFS and that its content is what we say it should be
    FileStatus[] statuses = fs.listStatus(outputDirPath);
    Assert.assertNotNull("No files found written to HDFS", statuses);
    Assert.assertEquals("Only one file expected", 1, statuses.length);

    for (FileStatus status : statuses) {
      Path filePath = status.getPath();
      logger.info("Found file on DFS: {}", filePath);
      FSDataInputStream stream = fs.open(filePath);
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
      String line = reader.readLine();
      logger.info("First line in file {}: {}", filePath, line);
      Assert.assertEquals(EVENT_BODY, line);
    }

    if (!KEEP_DATA) {
      fs.delete(outputDirPath, true);
    }

    cluster.shutdown();
    cluster = null;
  }

  /**
   * Writes two events in GZIP-compressed serialize.
   */
  @Test
  public void simpleHDFSGZipCompressedTest() throws EventDeliveryException, IOException {
    cluster = new MiniDFSCluster(new Configuration(), 1, true, null);
    cluster.waitActive();

    String outputDir = "/flume/simpleHDFSGZipCompressedTest";
    Path outputDirPath = new Path(outputDir);

    logger.info("Running test with output dir: {}", outputDir);

    FileSystem fs = cluster.getFileSystem();
    // ensure output directory is empty
    if (fs.exists(outputDirPath)) {
      fs.delete(outputDirPath, true);
    }

    String nnURL = getNameNodeURL(cluster);
    logger.info("Namenode address: {}", nnURL);

    Context chanCtx = new Context();
    MemoryChannel channel = new MemoryChannel();
    channel.setName("simpleHDFSTest-mem-chan");
    channel.configure(chanCtx);
    channel.start();

    Context sinkCtx = new Context();
    sinkCtx.put("hdfs.path", nnURL + outputDir);
    sinkCtx.put("hdfs.fileType", HDFSWriterFactory.CompStreamType);
    sinkCtx.put("hdfs.batchSize", Integer.toString(1));
    sinkCtx.put("hdfs.codeC", "gzip");

    HDFSEventSink sink = new HDFSEventSink();
    sink.setName("simpleHDFSTest-hdfs-sink");
    sink.configure(sinkCtx);
    sink.setChannel(channel);
    sink.start();

    // create an event
    String EVENT_BODY_1 = "yarg1";
    String EVENT_BODY_2 = "yarg2";
    channel.getTransaction().begin();
    try {
      channel.put(EventBuilder.withBody(EVENT_BODY_1, Charsets.UTF_8));
      channel.put(EventBuilder.withBody(EVENT_BODY_2, Charsets.UTF_8));
      channel.getTransaction().commit();
    } finally {
      channel.getTransaction().close();
    }

    // store event to HDFS
    sink.process();

    // shut down flume
    sink.stop();
    channel.stop();

    // verify that it's in HDFS and that its content is what we say it should be
    FileStatus[] statuses = fs.listStatus(outputDirPath);
    Assert.assertNotNull("No files found written to HDFS", statuses);
    Assert.assertEquals("Only one file expected", 1, statuses.length);

    for (FileStatus status : statuses) {
      Path filePath = status.getPath();
      logger.info("Found file on DFS: {}", filePath);
      FSDataInputStream stream = fs.open(filePath);
      BufferedReader reader = new BufferedReader(new InputStreamReader(
          new GZIPInputStream(stream)));
      String line = reader.readLine();
      logger.info("First line in file {}: {}", filePath, line);
      Assert.assertEquals(EVENT_BODY_1, line);

      // The rest of this test is commented-out (will fail) for 2 reasons:
      //
      // (1) At the time of this writing, Hadoop has a bug which causes the
      // non-native gzip implementation to create invalid gzip files when
      // finish() and resetState() are called. See HADOOP-8522.
      //
      // (2) Even if HADOOP-8522 is fixed, the JDK GZipInputStream is unable
      // to read multi-member (concatenated) gzip files. See this Sun bug:
      // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4691425
      //
      //line = reader.readLine();
      //logger.info("Second line in file {}: {}", filePath, line);
      //Assert.assertEquals(EVENT_BODY_2, line);
    }

    if (!KEEP_DATA) {
      fs.delete(outputDirPath, true);
    }

    cluster.shutdown();
    cluster = null;
  }

  /**
   * This is a very basic test that writes one event to HDFS and reads it back.
   */
  @Test
  public void underReplicationTest() throws EventDeliveryException,
      IOException {
    Configuration conf = new Configuration();
    conf.set("dfs.replication", String.valueOf(3));
    cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();

    String outputDir = "/flume/underReplicationTest";
    Path outputDirPath = new Path(outputDir);

    logger.info("Running test with output dir: {}", outputDir);

    FileSystem fs = cluster.getFileSystem();
    // ensure output directory is empty
    if (fs.exists(outputDirPath)) {
      fs.delete(outputDirPath, true);
    }

    String nnURL = getNameNodeURL(cluster);
    logger.info("Namenode address: {}", nnURL);

    Context chanCtx = new Context();
    MemoryChannel channel = new MemoryChannel();
    channel.setName("simpleHDFSTest-mem-chan");
    channel.configure(chanCtx);
    channel.start();

    Context sinkCtx = new Context();
    sinkCtx.put("hdfs.path", nnURL + outputDir);
    sinkCtx.put("hdfs.fileType", HDFSWriterFactory.DataStreamType);
    sinkCtx.put("hdfs.batchSize", Integer.toString(1));

    HDFSEventSink sink = new HDFSEventSink();
    sink.setName("simpleHDFSTest-hdfs-sink");
    sink.configure(sinkCtx);
    sink.setChannel(channel);
    sink.start();

    // create an event
    channel.getTransaction().begin();
    try {
      channel.put(EventBuilder.withBody("yarg 1", Charsets.UTF_8));
      channel.put(EventBuilder.withBody("yarg 2", Charsets.UTF_8));
      channel.put(EventBuilder.withBody("yarg 3", Charsets.UTF_8));
      channel.put(EventBuilder.withBody("yarg 4", Charsets.UTF_8));
      channel.put(EventBuilder.withBody("yarg 5", Charsets.UTF_8));
      channel.put(EventBuilder.withBody("yarg 5", Charsets.UTF_8));
      channel.getTransaction().commit();
    } finally {
      channel.getTransaction().close();
    }

    // store events to HDFS
    logger.info("Running process(). Create new file.");
    sink.process(); // create new file;
    logger.info("Running process(). Same file.");
    sink.process();

    // kill a datanode
    logger.info("Killing datanode #1...");
    cluster.stopDataNode(0);

    // there is a race here.. the client may or may not notice that the
    // datanode is dead before it next sync()s.
    // so, this next call may or may not roll a new file.

    logger.info("Running process(). Create new file? (racy)");
    sink.process();

    logger.info("Running process(). Create new file.");
    sink.process();

    logger.info("Running process(). Create new file.");
    sink.process();

    logger.info("Running process(). Create new file.");
    sink.process();

    // shut down flume
    sink.stop();
    channel.stop();

    // verify that it's in HDFS and that its content is what we say it should be
    FileStatus[] statuses = fs.listStatus(outputDirPath);
    Assert.assertNotNull("No files found written to HDFS", statuses);

    for (FileStatus status : statuses) {
      Path filePath = status.getPath();
      logger.info("Found file on DFS: {}", filePath);
      FSDataInputStream stream = fs.open(filePath);
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
      String line = reader.readLine();
      logger.info("First line in file {}: {}", filePath, line);
      Assert.assertTrue(line.startsWith("yarg"));
    }

    Assert.assertTrue("4 or 5 files expected, found " + statuses.length,
        statuses.length == 4 || statuses.length == 5);
    System.out.println("There are " + statuses.length + " files.");

    if (!KEEP_DATA) {
      fs.delete(outputDirPath, true);
    }

    cluster.shutdown();
    cluster = null;
  }

  /**
   * This is a very basic test that writes one event to HDFS and reads it back.
   */
  @Ignore("This test is flakey and causes tests to fail pretty often.")
  @Test
  public void maxUnderReplicationTest() throws EventDeliveryException,
      IOException {
    Configuration conf = new Configuration();
    conf.set("dfs.replication", String.valueOf(3));
    cluster = new MiniDFSCluster(conf, 3, true, null);
    cluster.waitActive();

    String outputDir = "/flume/underReplicationTest";
    Path outputDirPath = new Path(outputDir);

    logger.info("Running test with output dir: {}", outputDir);

    FileSystem fs = cluster.getFileSystem();
    // ensure output directory is empty
    if (fs.exists(outputDirPath)) {
      fs.delete(outputDirPath, true);
    }

    String nnURL = getNameNodeURL(cluster);
    logger.info("Namenode address: {}", nnURL);

    Context chanCtx = new Context();
    MemoryChannel channel = new MemoryChannel();
    channel.setName("simpleHDFSTest-mem-chan");
    channel.configure(chanCtx);
    channel.start();

    Context sinkCtx = new Context();
    sinkCtx.put("hdfs.path", nnURL + outputDir);
    sinkCtx.put("hdfs.fileType", HDFSWriterFactory.DataStreamType);
    sinkCtx.put("hdfs.batchSize", Integer.toString(1));

    HDFSEventSink sink = new HDFSEventSink();
    sink.setName("simpleHDFSTest-hdfs-sink");
    sink.configure(sinkCtx);
    sink.setChannel(channel);
    sink.start();

    // create an event
    channel.getTransaction().begin();
    try {
      for (int i = 0; i < 50; i++) {
        channel.put(EventBuilder.withBody("yarg " + i, Charsets.UTF_8));
      }
      channel.getTransaction().commit();
    } finally {
      channel.getTransaction().close();
    }

    // store events to HDFS
    logger.info("Running process(). Create new file.");
    sink.process(); // create new file;
    logger.info("Running process(). Same file.");
    sink.process();

    // kill a datanode
    logger.info("Killing datanode #1...");
    cluster.stopDataNode(0);

    // there is a race here.. the client may or may not notice that the
    // datanode is dead before it next sync()s.
    // so, this next call may or may not roll a new file.

    logger.info("Running process(). Create new file? (racy)");
    sink.process();

    for (int i = 3; i < 50; i++) {
      logger.info("Running process().");
      sink.process();
    }

    // shut down flume
    sink.stop();
    channel.stop();

    // verify that it's in HDFS and that its content is what we say it should be
    FileStatus[] statuses = fs.listStatus(outputDirPath);
    Assert.assertNotNull("No files found written to HDFS", statuses);

    for (FileStatus status : statuses) {
      Path filePath = status.getPath();
      logger.info("Found file on DFS: {}", filePath);
      FSDataInputStream stream = fs.open(filePath);
      BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
      String line = reader.readLine();
      logger.info("First line in file {}: {}", filePath, line);
      Assert.assertTrue(line.startsWith("yarg"));
    }

    System.out.println("There are " + statuses.length + " files.");
    Assert.assertEquals("31 files expected, found " + statuses.length,
        31, statuses.length);

    if (!KEEP_DATA) {
      fs.delete(outputDirPath, true);
    }

    cluster.shutdown();
    cluster = null;
  }

  /**
   * Tests if the lease gets released if the close() call throws IOException.
   * For more details see https://issues.apache.org/jira/browse/FLUME-3080
   */
  @Test
  public void testLeaseRecoveredIfCloseThrowsIOException() throws Exception {
    testLeaseRecoveredIfCloseFails(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        throw new IOException();
      }
    });
  }

  /**
   * Tests if the lease gets released if the close() call times out.
   * For more details see https://issues.apache.org/jira/browse/FLUME-3080
   */
  @Test
  public void testLeaseRecoveredIfCloseTimesOut() throws Exception {
    testLeaseRecoveredIfCloseFails(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        TimeUnit.SECONDS.sleep(30);
        return null;
      }
    });
  }

  private void testLeaseRecoveredIfCloseFails(final Callable<?> doThisInClose)
      throws Exception {
    cluster = new MiniDFSCluster.Builder(new Configuration()).numDataNodes(1).format(true).build();
    cluster.waitActive();

    String outputDir = "/flume/leaseRecovery";
    Path outputDirPath = new Path(outputDir);

    logger.info("Running test with output dir: {}", outputDir);

    FileSystem fs = cluster.getFileSystem();
    // ensure output directory is empty
    if (fs.exists(outputDirPath)) {
      fs.delete(outputDirPath, true);
    }
    String nnURL = getNameNodeURL(cluster);

    Context ctx = new Context();
    MemoryChannel channel = new MemoryChannel();
    channel.configure(ctx);
    channel.start();

    ctx.put("hdfs.path", nnURL + outputDir);
    ctx.put("hdfs.fileType", HDFSWriterFactory.DataStreamType);
    ctx.put("hdfs.batchSize", Integer.toString(1));
    ctx.put("hdfs.callTimeout", Integer.toString(1000));

    HDFSWriter hdfsWriter = new HDFSDataStream() {
      @Override
      public void close() throws IOException {
        try {
          doThisInClose.call();
        } catch (Throwable e) {
          Throwables.propagateIfPossible(e, IOException.class);
          throw new RuntimeException(e);
        }
      }
    };
    hdfsWriter.configure(ctx);

    HDFSEventSink sink = new HDFSEventSink();
    sink.configure(ctx);
    sink.setMockFs(fs);
    sink.setMockWriter(hdfsWriter);
    sink.setChannel(channel);
    sink.start();

    Transaction txn = channel.getTransaction();
    txn.begin();
    try {
      channel.put(EventBuilder.withBody("test", Charsets.UTF_8));
      txn.commit();
    } finally {
      txn.close();
    }

    sink.process();
    sink.stop();
    channel.stop();

    FileStatus[] statuses = fs.listStatus(outputDirPath);
    Assert.assertEquals(1, statuses.length);

    String filePath = statuses[0].getPath().toUri().getPath();

    // -1 in case that the lease doesn't exist.
    long leaseRenewalTime = NameNodeAdapter.getLeaseRenewalTime(cluster.getNameNode(), filePath);
    // wait until the NameNode recovers the lease
    for (int i = 0; (i < 10) && (leaseRenewalTime != -1L); i++) {
      TimeUnit.SECONDS.sleep(1);
      leaseRenewalTime = NameNodeAdapter.getLeaseRenewalTime(cluster.getNameNode(), filePath);
    }

    // There should be no lease for the given path even if close failed as the BucketWriter
    // explicitly calls the recoverLease()
    Assert.assertEquals(-1L, leaseRenewalTime);

    if (!KEEP_DATA) {
      fs.delete(outputDirPath, true);
    }

    cluster.shutdown();
    cluster = null;
  }

  @AfterClass
  public static void teardownClass() {
    // restore system state, if needed
    if (oldTestBuildDataProp != null) {
      System.setProperty(TEST_BUILD_DATA_KEY, oldTestBuildDataProp);
    }

    if (!KEEP_DATA) {
      FileUtils.deleteQuietly(new File(DFS_DIR));
    }
  }

}
