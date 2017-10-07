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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.CharsetDecoder;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Clock;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink.Status;
import org.apache.flume.SystemClock;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

public class TestHDFSEventSink {

  private HDFSEventSink sink;
  private String testPath;
  private static final Logger LOG = LoggerFactory
      .getLogger(HDFSEventSink.class);

  static {
    System.setProperty("java.security.krb5.realm", "flume");
    System.setProperty("java.security.krb5.kdc", "blah");
  }

  private void dirCleanup() {
    Configuration conf = new Configuration();
    try {
      FileSystem fs = FileSystem.get(conf);
      Path dirPath = new Path(testPath);
      if (fs.exists(dirPath)) {
        fs.delete(dirPath, true);
      }
    } catch (IOException eIO) {
      LOG.warn("IO Error in test cleanup", eIO);
    }
  }

  // TODO: use System.getProperty("file.separator") instead of hardcoded '/'
  @Before
  public void setUp() {
    LOG.debug("Starting...");
    /*
     * FIXME: Use a dynamic path to support concurrent test execution. Also,
     * beware of the case where this path is used for something or when the
     * Hadoop config points at file:/// rather than hdfs://. We need to find a
     * better way of testing HDFS related functionality.
     */
    testPath = "file:///tmp/flume-test."
        + Calendar.getInstance().getTimeInMillis() + "."
        + Thread.currentThread().getId();

    sink = new HDFSEventSink();
    sink.setName("HDFSEventSink-" + UUID.randomUUID().toString());
    dirCleanup();
  }

  @After
  public void tearDown() {
    if (System.getenv("hdfs_keepFiles") == null) dirCleanup();
  }

  @Test
  public void testTextBatchAppend() throws Exception {
    doTestTextBatchAppend(false);
  }

  @Test
  public void testTextBatchAppendRawFS() throws Exception {
    doTestTextBatchAppend(true);
  }

  public void doTestTextBatchAppend(boolean useRawLocalFileSystem)
      throws Exception {
    LOG.debug("Starting...");

    final long rollCount = 10;
    final long batchSize = 2;
    final String fileName = "FlumeData";
    String newPath = testPath + "/singleTextBucket";
    int totalEvents = 0;
    int i = 1, j = 1;

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(newPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();

    // context.put("hdfs.path", testPath + "/%Y-%m-%d/%H");
    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.rollInterval", "0");
    context.put("hdfs.rollSize", "0");
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.writeFormat", "Text");
    context.put("hdfs.useRawLocalFileSystem",
        Boolean.toString(useRawLocalFileSystem));
    context.put("hdfs.fileType", "DataStream");

    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    Calendar eventDate = Calendar.getInstance();
    List<String> bodies = Lists.newArrayList();

    // push the event batches into channel to roll twice
    for (i = 1; i <= (rollCount * 10) / batchSize; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (j = 1; j <= batchSize; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        String body = "Test." + i + "." + j;
        event.setBody(body.getBytes());
        bodies.add(body);
        channel.put(event);
        totalEvents++;
      }
      txn.commit();
      txn.close();

      // execute sink to process the events
      sink.process();
    }

    sink.stop();

    // loop through all the files generated and check their contains
    FileStatus[] dirStat = fs.listStatus(dirPath);
    Path[] fList = FileUtil.stat2Paths(dirStat);

    // check that the roll happened correctly for the given data
    long expectedFiles = totalEvents / rollCount;
    if (totalEvents % rollCount > 0) expectedFiles++;
    Assert.assertEquals("num files wrong, found: " +
        Lists.newArrayList(fList), expectedFiles, fList.length);
    // check the contents of the all files
    verifyOutputTextFiles(fs, conf, dirPath.toUri().getPath(), fileName, bodies);
  }

  @Test
  public void testLifecycle() throws InterruptedException, LifecycleException {
    LOG.debug("Starting...");
    Context context = new Context();

    context.put("hdfs.path", testPath);
    /*
     * context.put("hdfs.rollInterval", String.class);
     * context.get("hdfs.rollSize", String.class); context.get("hdfs.rollCount",
     * String.class);
     */
    Configurables.configure(sink, context);

    sink.setChannel(new MemoryChannel());

    sink.start();
    sink.stop();
  }

  @Test
  public void testEmptyChannelResultsInStatusBackoff()
      throws InterruptedException, LifecycleException, EventDeliveryException {
    LOG.debug("Starting...");
    Context context = new Context();
    Channel channel = new MemoryChannel();
    context.put("hdfs.path", testPath);
    context.put("keep-alive", "0");
    Configurables.configure(sink, context);
    Configurables.configure(channel, context);
    sink.setChannel(channel);
    sink.start();
    Assert.assertEquals(Status.BACKOFF, sink.process());
    sink.stop();
  }

  @Test
  public void testKerbFileAccess() throws InterruptedException,
      LifecycleException, EventDeliveryException, IOException {
    LOG.debug("Starting testKerbFileAccess() ...");
    final String fileName = "FlumeData";
    final long rollCount = 5;
    final long batchSize = 2;
    String newPath = testPath + "/singleBucket";
    String kerbConfPrincipal = "user1/localhost@EXAMPLE.COM";
    String kerbKeytab = "/usr/lib/flume/nonexistkeytabfile";

    //turn security on
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);

    Context context = new Context();
    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.kerberosPrincipal", kerbConfPrincipal);
    context.put("hdfs.kerberosKeytab", kerbKeytab);

    try {
      Configurables.configure(sink, context);
      Assert.fail("no exception thrown");
    } catch (IllegalArgumentException expected) {
      Assert.assertTrue(expected.getMessage().contains(
          "Keytab is not a readable file"));
    } finally {
      //turn security off
      conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION,
          "simple");
      UserGroupInformation.setConfiguration(conf);
    }
  }

  @Test
  public void testTextAppend() throws InterruptedException, LifecycleException,
      EventDeliveryException, IOException {

    LOG.debug("Starting...");
    final long rollCount = 3;
    final long batchSize = 2;
    final String fileName = "FlumeData";
    String newPath = testPath + "/singleTextBucket";
    int totalEvents = 0;
    int i = 1, j = 1;

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(newPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();

    // context.put("hdfs.path", testPath + "/%Y-%m-%d/%H");
    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.writeFormat", "Text");
    context.put("hdfs.fileType", "DataStream");

    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    Calendar eventDate = Calendar.getInstance();
    List<String> bodies = Lists.newArrayList();

    // push the event batches into channel
    for (i = 1; i < 4; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (j = 1; j <= batchSize; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);
        String body = "Test." + i + "." + j;
        event.setBody(body.getBytes());
        bodies.add(body);
        channel.put(event);
        totalEvents++;
      }
      txn.commit();
      txn.close();

      // execute sink to process the events
      sink.process();
    }

    sink.stop();

    // loop through all the files generated and check their contains
    FileStatus[] dirStat = fs.listStatus(dirPath);
    Path[] fList = FileUtil.stat2Paths(dirStat);

    // check that the roll happened correctly for the given data
    long expectedFiles = totalEvents / rollCount;
    if (totalEvents % rollCount > 0) expectedFiles++;
    Assert.assertEquals("num files wrong, found: " +
        Lists.newArrayList(fList), expectedFiles, fList.length);
    verifyOutputTextFiles(fs, conf, dirPath.toUri().getPath(), fileName, bodies);
  }

  @Test
  public void testAvroAppend() throws InterruptedException, LifecycleException,
      EventDeliveryException, IOException {

    LOG.debug("Starting...");
    final long rollCount = 3;
    final long batchSize = 2;
    final String fileName = "FlumeData";
    String newPath = testPath + "/singleTextBucket";
    int totalEvents = 0;
    int i = 1, j = 1;

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(newPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();

    // context.put("hdfs.path", testPath + "/%Y-%m-%d/%H");
    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.writeFormat", "Text");
    context.put("hdfs.fileType", "DataStream");
    context.put("serializer", "AVRO_EVENT");

    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    Calendar eventDate = Calendar.getInstance();
    List<String> bodies = Lists.newArrayList();

    // push the event batches into channel
    for (i = 1; i < 4; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (j = 1; j <= batchSize; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);
        String body = "Test." + i + "." + j;
        event.setBody(body.getBytes());
        bodies.add(body);
        channel.put(event);
        totalEvents++;
      }
      txn.commit();
      txn.close();

      // execute sink to process the events
      sink.process();
    }

    sink.stop();

    // loop through all the files generated and check their contains
    FileStatus[] dirStat = fs.listStatus(dirPath);
    Path[] fList = FileUtil.stat2Paths(dirStat);

    // check that the roll happened correctly for the given data
    long expectedFiles = totalEvents / rollCount;
    if (totalEvents % rollCount > 0) expectedFiles++;
    Assert.assertEquals("num files wrong, found: " +
        Lists.newArrayList(fList), expectedFiles, fList.length);
    verifyOutputAvroFiles(fs, conf, dirPath.toUri().getPath(), fileName, bodies);
  }

  @Test
  public void testSimpleAppend() throws InterruptedException,
      LifecycleException, EventDeliveryException, IOException {

    LOG.debug("Starting...");
    final String fileName = "FlumeData";
    final long rollCount = 5;
    final long batchSize = 2;
    final int numBatches = 4;
    String newPath = testPath + "/singleBucket";
    int totalEvents = 0;
    int i = 1, j = 1;

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(newPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();

    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));

    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    Calendar eventDate = Calendar.getInstance();
    List<String> bodies = Lists.newArrayList();

    // push the event batches into channel
    for (i = 1; i < numBatches; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (j = 1; j <= batchSize; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);
        String body = "Test." + i + "." + j;
        event.setBody(body.getBytes());
        bodies.add(body);
        channel.put(event);
        totalEvents++;
      }
      txn.commit();
      txn.close();

      // execute sink to process the events
      sink.process();
    }

    sink.stop();

    // loop through all the files generated and check their contains
    FileStatus[] dirStat = fs.listStatus(dirPath);
    Path[] fList = FileUtil.stat2Paths(dirStat);

    // check that the roll happened correctly for the given data
    long expectedFiles = totalEvents / rollCount;
    if (totalEvents % rollCount > 0) expectedFiles++;
    Assert.assertEquals("num files wrong, found: " +
        Lists.newArrayList(fList), expectedFiles, fList.length);
    verifyOutputSequenceFiles(fs, conf, dirPath.toUri().getPath(), fileName, bodies);
  }

  @Test
  public void testSimpleAppendLocalTime()
      throws InterruptedException, LifecycleException, EventDeliveryException, IOException {
    final long currentTime = System.currentTimeMillis();
    Clock clk = new Clock() {
      @Override
      public long currentTimeMillis() {
        return currentTime;
      }
    };

    LOG.debug("Starting...");
    final String fileName = "FlumeData";
    final long rollCount = 5;
    final long batchSize = 2;
    final int numBatches = 4;
    String newPath = testPath + "/singleBucket/%s" ;
    String expectedPath = testPath + "/singleBucket/" +
        String.valueOf(currentTime / 1000);
    int totalEvents = 0;
    int i = 1, j = 1;

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(expectedPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();

    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.useLocalTimeStamp", String.valueOf(true));

    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.setBucketClock(clk);
    sink.start();

    Calendar eventDate = Calendar.getInstance();
    List<String> bodies = Lists.newArrayList();

    // push the event batches into channel
    for (i = 1; i < numBatches; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (j = 1; j <= batchSize; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);
        String body = "Test." + i + "." + j;
        event.setBody(body.getBytes());
        bodies.add(body);
        channel.put(event);
        totalEvents++;
      }
      txn.commit();
      txn.close();

      // execute sink to process the events
      sink.process();
    }

    sink.stop();

    // loop through all the files generated and check their contains
    FileStatus[] dirStat = fs.listStatus(dirPath);
    Path[] fList = FileUtil.stat2Paths(dirStat);

    // check that the roll happened correctly for the given data
    long expectedFiles = totalEvents / rollCount;
    if (totalEvents % rollCount > 0) expectedFiles++;
    Assert.assertEquals("num files wrong, found: " +
        Lists.newArrayList(fList), expectedFiles, fList.length);
    verifyOutputSequenceFiles(fs, conf, dirPath.toUri().getPath(), fileName, bodies);
    // The clock in bucketpath is static, so restore the real clock
    sink.setBucketClock(new SystemClock());
  }

  @Test
  public void testAppend() throws InterruptedException, LifecycleException,
      EventDeliveryException, IOException {

    LOG.debug("Starting...");
    final long rollCount = 3;
    final long batchSize = 2;
    final String fileName = "FlumeData";

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(testPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();

    context.put("hdfs.path", testPath + "/%Y-%m-%d/%H");
    context.put("hdfs.timeZone", "UTC");
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));

    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    Calendar eventDate = Calendar.getInstance();
    List<String> bodies = Lists.newArrayList();
    // push the event batches into channel
    for (int i = 1; i < 4; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (int j = 1; j <= batchSize; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);
        String body = "Test." + i + "." + j;
        event.setBody(body.getBytes());
        bodies.add(body);
        channel.put(event);
      }
      txn.commit();
      txn.close();

      // execute sink to process the events
      sink.process();
    }

    sink.stop();
    verifyOutputSequenceFiles(fs, conf, dirPath.toUri().getPath(), fileName, bodies);
  }

  // inject fault and make sure that the txn is rolled back and retried
  @Test
  public void testBadSimpleAppend() throws InterruptedException,
      LifecycleException, EventDeliveryException, IOException {

    LOG.debug("Starting...");
    final String fileName = "FlumeData";
    final long rollCount = 5;
    final long batchSize = 2;
    final int numBatches = 4;
    String newPath = testPath + "/singleBucket";
    int totalEvents = 0;
    int i = 1, j = 1;

    HDFSTestWriterFactory badWriterFactory = new HDFSTestWriterFactory();
    sink = new HDFSEventSink(badWriterFactory);

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(newPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();

    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.fileType", HDFSTestWriterFactory.TestSequenceFileType);

    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    Calendar eventDate = Calendar.getInstance();

    List<String> bodies = Lists.newArrayList();
    // push the event batches into channel
    for (i = 1; i < numBatches; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (j = 1; j <= batchSize; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);

        String body = "Test." + i + "." + j;
        event.setBody(body.getBytes());
        bodies.add(body);
        // inject fault
        if ((totalEvents % 30) == 1) {
          event.getHeaders().put("fault-once", "");
        }
        channel.put(event);
        totalEvents++;
      }
      txn.commit();
      txn.close();

      LOG.info("Process events: " + sink.process());
    }
    LOG.info("Process events to end of transaction max: " + sink.process());
    LOG.info("Process events to injected fault: " + sink.process());
    LOG.info("Process events remaining events: " + sink.process());
    sink.stop();
    verifyOutputSequenceFiles(fs, conf, dirPath.toUri().getPath(), fileName, bodies);

  }


  private List<String> getAllFiles(String input) {
    List<String> output = Lists.newArrayList();
    File dir = new File(input);
    if (dir.isFile()) {
      output.add(dir.getAbsolutePath());
    } else if (dir.isDirectory()) {
      for (String file : dir.list()) {
        File subDir = new File(dir, file);
        output.addAll(getAllFiles(subDir.getAbsolutePath()));
      }
    }
    return output;
  }

  private void verifyOutputSequenceFiles(FileSystem fs, Configuration conf, String dir,
                                         String prefix, List<String> bodies) throws IOException {
    int found = 0;
    int expected = bodies.size();
    for (String outputFile : getAllFiles(dir)) {
      String name = (new File(outputFile)).getName();
      if (name.startsWith(prefix)) {
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(outputFile), conf);
        LongWritable key = new LongWritable();
        BytesWritable value = new BytesWritable();
        while (reader.next(key, value)) {
          String body = new String(value.getBytes(), 0, value.getLength());
          if (bodies.contains(body)) {
            LOG.debug("Found event body: {}", body);
            bodies.remove(body);
            found++;
          }
        }
        reader.close();
      }
    }
    if (!bodies.isEmpty()) {
      for (String body : bodies) {
        LOG.error("Never found event body: {}", body);
      }
    }
    Assert.assertTrue("Found = " + found + ", Expected = "  +
        expected + ", Left = " + bodies.size() + " " + bodies,
          bodies.size() == 0);

  }

  private void verifyOutputTextFiles(FileSystem fs, Configuration conf, String dir, String prefix,
                                     List<String> bodies) throws IOException {
    int found = 0;
    int expected = bodies.size();
    for (String outputFile : getAllFiles(dir)) {
      String name = (new File(outputFile)).getName();
      if (name.startsWith(prefix)) {
        FSDataInputStream input = fs.open(new Path(outputFile));
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String body = null;
        while ((body = reader.readLine()) != null) {
          bodies.remove(body);
          found++;
        }
        reader.close();
      }
    }
    Assert.assertTrue("Found = " + found + ", Expected = "  +
        expected + ", Left = " + bodies.size() + " " + bodies,
          bodies.size() == 0);

  }

  private void verifyOutputAvroFiles(FileSystem fs, Configuration conf, String dir, String prefix,
                                     List<String> bodies) throws IOException {
    int found = 0;
    int expected = bodies.size();
    for (String outputFile : getAllFiles(dir)) {
      String name = (new File(outputFile)).getName();
      if (name.startsWith(prefix)) {
        FSDataInputStream input = fs.open(new Path(outputFile));
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        DataFileStream<GenericRecord> avroStream =
            new DataFileStream<GenericRecord>(input, reader);
        GenericRecord record = new GenericData.Record(avroStream.getSchema());
        while (avroStream.hasNext()) {
          avroStream.next(record);
          ByteBuffer body = (ByteBuffer) record.get("body");
          CharsetDecoder decoder = Charsets.UTF_8.newDecoder();
          String bodyStr = decoder.decode(body).toString();
          LOG.debug("Removing event: {}", bodyStr);
          bodies.remove(bodyStr);
          found++;
        }
        avroStream.close();
        input.close();
      }
    }
    Assert.assertTrue("Found = " + found + ", Expected = "  +
        expected + ", Left = " + bodies.size() + " " + bodies,
            bodies.size() == 0);
  }

  /**
   * Ensure that when a write throws an IOException we are
   * able to continue to progress in the next process() call.
   * This relies on Transactional rollback semantics for durability and
   * the behavior of the BucketWriter class of close()ing upon IOException.
   */
  @Test
  public void testCloseReopen()
      throws InterruptedException, LifecycleException, EventDeliveryException, IOException {

    LOG.debug("Starting...");
    final int numBatches = 4;
    final String fileName = "FlumeData";
    final long rollCount = 5;
    final long batchSize = 2;
    String newPath = testPath + "/singleBucket";
    int i = 1, j = 1;

    HDFSTestWriterFactory badWriterFactory = new HDFSTestWriterFactory();
    sink = new HDFSEventSink(badWriterFactory);

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(newPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();

    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.fileType", HDFSTestWriterFactory.TestSequenceFileType);

    Configurables.configure(sink, context);

    MemoryChannel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());

    sink.setChannel(channel);
    sink.start();

    Calendar eventDate = Calendar.getInstance();
    List<String> bodies = Lists.newArrayList();
    // push the event batches into channel
    for (i = 1; i < numBatches; i++) {
      channel.getTransaction().begin();
      try {
        for (j = 1; j <= batchSize; j++) {
          Event event = new SimpleEvent();
          eventDate.clear();
          eventDate.set(2011, i, i, i, 0); // yy mm dd
          event.getHeaders().put("timestamp",
              String.valueOf(eventDate.getTimeInMillis()));
          event.getHeaders().put("hostname", "Host" + i);
          String body = "Test." + i + "." + j;
          event.setBody(body.getBytes());
          bodies.add(body);
          // inject fault
          event.getHeaders().put("fault-until-reopen", "");
          channel.put(event);
        }
        channel.getTransaction().commit();
      } finally {
        channel.getTransaction().close();
      }
      LOG.info("execute sink to process the events: " + sink.process());
    }
    LOG.info("clear any events pending due to errors: " + sink.process());
    sink.stop();

    verifyOutputSequenceFiles(fs, conf, dirPath.toUri().getPath(), fileName, bodies);
  }

  /**
   * Test that the old bucket writer is closed at the end of rollInterval and
   * a new one is used for the next set of events.
   */
  @Test
  public void testCloseReopenOnRollTime()
      throws InterruptedException, LifecycleException, EventDeliveryException, IOException {

    LOG.debug("Starting...");
    final int numBatches = 4;
    final String fileName = "FlumeData";
    final long batchSize = 2;
    String newPath = testPath + "/singleBucket";
    int i = 1, j = 1;

    HDFSTestWriterFactory badWriterFactory = new HDFSTestWriterFactory();
    sink = new HDFSEventSink(badWriterFactory);

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(newPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();

    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(0));
    context.put("hdfs.rollSize", String.valueOf(0));
    context.put("hdfs.rollInterval", String.valueOf(2));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.fileType", HDFSTestWriterFactory.TestSequenceFileType);

    Configurables.configure(sink, context);

    MemoryChannel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());

    sink.setChannel(channel);
    sink.start();

    Calendar eventDate = Calendar.getInstance();
    List<String> bodies = Lists.newArrayList();
    // push the event batches into channel
    for (i = 1; i < numBatches; i++) {
      channel.getTransaction().begin();
      try {
        for (j = 1; j <= batchSize; j++) {
          Event event = new SimpleEvent();
          eventDate.clear();
          eventDate.set(2011, i, i, i, 0); // yy mm dd
          event.getHeaders().put("timestamp",
              String.valueOf(eventDate.getTimeInMillis()));
          event.getHeaders().put("hostname", "Host" + i);
          String body = "Test." + i + "." + j;
          event.setBody(body.getBytes());
          bodies.add(body);
          // inject fault
          event.getHeaders().put("count-check", "");
          channel.put(event);
        }
        channel.getTransaction().commit();
      } finally {
        channel.getTransaction().close();
      }
      LOG.info("execute sink to process the events: " + sink.process());
      // Make sure the first file gets rolled due to rollTimeout.
      if (i == 1) {
        Thread.sleep(2001);
      }
    }
    LOG.info("clear any events pending due to errors: " + sink.process());
    sink.stop();

    Assert.assertTrue(badWriterFactory.openCount.get() >= 2);
    LOG.info("Total number of bucket writers opened: {}",
        badWriterFactory.openCount.get());
    verifyOutputSequenceFiles(fs, conf, dirPath.toUri().getPath(), fileName,
        bodies);
  }

  /**
   * Test that a close due to roll interval removes the bucketwriter from
   * sfWriters map.
   */
  @Test
  public void testCloseRemovesFromSFWriters()
      throws InterruptedException, LifecycleException, EventDeliveryException, IOException {

    LOG.debug("Starting...");
    final String fileName = "FlumeData";
    final long batchSize = 2;
    String newPath = testPath + "/singleBucket";
    int i = 1, j = 1;

    HDFSTestWriterFactory badWriterFactory = new HDFSTestWriterFactory();
    sink = new HDFSEventSink(badWriterFactory);

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(newPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();

    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(0));
    context.put("hdfs.rollSize", String.valueOf(0));
    context.put("hdfs.rollInterval", String.valueOf(1));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.fileType", HDFSTestWriterFactory.TestSequenceFileType);
    String expectedLookupPath = newPath + "/FlumeData";

    Configurables.configure(sink, context);

    MemoryChannel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());

    sink.setChannel(channel);
    sink.start();

    Calendar eventDate = Calendar.getInstance();
    List<String> bodies = Lists.newArrayList();
    // push the event batches into channel
    channel.getTransaction().begin();
    try {
      for (j = 1; j <= 2 * batchSize; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);
        String body = "Test." + i + "." + j;
        event.setBody(body.getBytes());
        bodies.add(body);
        // inject fault
        event.getHeaders().put("count-check", "");
        channel.put(event);
      }
      channel.getTransaction().commit();
    } finally {
      channel.getTransaction().close();
    }
    LOG.info("execute sink to process the events: " + sink.process());
    Assert.assertTrue(sink.getSfWriters().containsKey(expectedLookupPath));
    // Make sure the first file gets rolled due to rollTimeout.
    Thread.sleep(2001);
    Assert.assertFalse(sink.getSfWriters().containsKey(expectedLookupPath));
    LOG.info("execute sink to process the events: " + sink.process());
    // A new bucket writer should have been created for this bucket. So
    // sfWriters map should not have the same key again.
    Assert.assertTrue(sink.getSfWriters().containsKey(expectedLookupPath));
    sink.stop();

    LOG.info("Total number of bucket writers opened: {}",
        badWriterFactory.openCount.get());
    verifyOutputSequenceFiles(fs, conf, dirPath.toUri().getPath(), fileName,
        bodies);
  }



  /*
   * append using slow sink writer.
   * verify that the process returns backoff due to timeout
   */
  @Test
  public void testSlowAppendFailure() throws InterruptedException,
      LifecycleException, EventDeliveryException, IOException {

    LOG.debug("Starting...");
    final String fileName = "FlumeData";
    final long rollCount = 5;
    final long batchSize = 2;
    final int numBatches = 2;
    String newPath = testPath + "/singleBucket";
    int i = 1, j = 1;

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(newPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    // create HDFS sink with slow writer
    HDFSTestWriterFactory badWriterFactory = new HDFSTestWriterFactory();
    sink = new HDFSEventSink(badWriterFactory);

    Context context = new Context();
    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.fileType", HDFSTestWriterFactory.TestSequenceFileType);
    context.put("hdfs.callTimeout", Long.toString(1000));
    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    Calendar eventDate = Calendar.getInstance();

    // push the event batches into channel
    for (i = 0; i < numBatches; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (j = 1; j <= batchSize; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);
        event.getHeaders().put("slow", "1500");
        event.setBody(("Test." + i + "." + j).getBytes());
        channel.put(event);
      }
      txn.commit();
      txn.close();

      // execute sink to process the events
      Status satus = sink.process();

      // verify that the append returned backoff due to timeotu
      Assert.assertEquals(satus, Status.BACKOFF);
    }

    sink.stop();
  }

  /*
   * append using slow sink writer with specified append timeout
   * verify that the data is written correctly to files
   */
  private void slowAppendTestHelper(long appendTimeout)
      throws InterruptedException, IOException, LifecycleException, EventDeliveryException,
             IOException {
    final String fileName = "FlumeData";
    final long rollCount = 5;
    final long batchSize = 2;
    final int numBatches = 2;
    String newPath = testPath + "/singleBucket";
    int totalEvents = 0;
    int i = 1, j = 1;

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(newPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    // create HDFS sink with slow writer
    HDFSTestWriterFactory badWriterFactory = new HDFSTestWriterFactory();
    sink = new HDFSEventSink(badWriterFactory);

    Context context = new Context();
    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.fileType", HDFSTestWriterFactory.TestSequenceFileType);
    context.put("hdfs.appendTimeout", String.valueOf(appendTimeout));
    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    Calendar eventDate = Calendar.getInstance();
    List<String> bodies = Lists.newArrayList();
    // push the event batches into channel
    for (i = 0; i < numBatches; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      for (j = 1; j <= batchSize; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);
        event.getHeaders().put("slow", "1500");
        String body = "Test." + i + "." + j;
        event.setBody(body.getBytes());
        bodies.add(body);
        channel.put(event);
        totalEvents++;
      }
      txn.commit();
      txn.close();

      // execute sink to process the events
      sink.process();
    }

    sink.stop();

    // loop through all the files generated and check their contains
    FileStatus[] dirStat = fs.listStatus(dirPath);
    Path[] fList = FileUtil.stat2Paths(dirStat);

    // check that the roll happened correctly for the given data
    // Note that we'll end up with two files with only a head
    long expectedFiles = totalEvents / rollCount;
    if (totalEvents % rollCount > 0) expectedFiles++;
    Assert.assertEquals("num files wrong, found: " +
        Lists.newArrayList(fList), expectedFiles, fList.length);
    verifyOutputSequenceFiles(fs, conf, dirPath.toUri().getPath(), fileName, bodies);
  }

  /*
   * append using slow sink writer with long append timeout
   * verify that the data is written correctly to files
   */
  @Test
  public void testSlowAppendWithLongTimeout() throws InterruptedException,
      LifecycleException, EventDeliveryException, IOException {
    LOG.debug("Starting...");
    slowAppendTestHelper(3000);
  }

  /*
   * append using slow sink writer with no timeout to make append
   * synchronous. Verify that the data is written correctly to files
   */
  @Test
  public void testSlowAppendWithoutTimeout() throws InterruptedException,
      LifecycleException, EventDeliveryException, IOException {
    LOG.debug("Starting...");
    slowAppendTestHelper(0);
  }
  @Test
  public void testCloseOnIdle() throws IOException, EventDeliveryException, InterruptedException {
    String hdfsPath = testPath + "/idleClose";

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(hdfsPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);
    Context context = new Context();
    context.put("hdfs.path", hdfsPath);
    /*
     * All three rolling methods are disabled so the only
     * way a file can roll is through the idle timeout.
     */
    context.put("hdfs.rollCount", "0");
    context.put("hdfs.rollSize", "0");
    context.put("hdfs.rollInterval", "0");
    context.put("hdfs.batchSize", "2");
    context.put("hdfs.idleTimeout", "1");
    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    Transaction txn = channel.getTransaction();
    txn.begin();
    for (int i = 0; i < 10; i++) {
      Event event = new SimpleEvent();
      event.setBody(("test event " + i).getBytes());
      channel.put(event);
    }
    txn.commit();
    txn.close();

    sink.process();
    sink.process();
    Thread.sleep(1001);
    // previous file should have timed out now
    // this can throw BucketClosedException(from the bucketWriter having
    // closed),this is not an issue as the sink will retry and get a fresh
    // bucketWriter so long as the onClose handler properly removes
    // bucket writers that were closed.
    sink.process();
    sink.process();
    Thread.sleep(500); // shouldn't be enough for a timeout to occur
    sink.process();
    sink.process();
    sink.stop();
    FileStatus[] dirStat = fs.listStatus(dirPath);
    Path[] fList = FileUtil.stat2Paths(dirStat);
    Assert.assertEquals("Incorrect content of the directory " + StringUtils.join(fList, ","),
                        2, fList.length);
    Assert.assertTrue(!fList[0].getName().endsWith(".tmp") &&
                      !fList[1].getName().endsWith(".tmp"));
    fs.close();
  }

  /**
   * This test simulates what happens when a batch of events is written to a compressed sequence
   * file (and thus hsync'd to hdfs) but the file is not yet closed.
   *
   * When this happens, the data that we wrote should still be readable.
   */
  @Test
  public void testBlockCompressSequenceFileWriterSync() throws IOException, EventDeliveryException {
    String hdfsPath = testPath + "/sequenceFileWriterSync";
    FileSystem fs = FileSystem.get(new Configuration());
    // Since we are reading a partial file we don't want to use checksums
    fs.setVerifyChecksum(false);
    fs.setWriteChecksum(false);

    // Compression codecs that don't require native hadoop libraries
    String [] codecs = {"BZip2Codec", "DeflateCodec"};

    for (String codec : codecs) {
      sequenceFileWriteAndVerifyEvents(fs, hdfsPath, codec, Collections.singletonList(
          "single-event"
      ));

      sequenceFileWriteAndVerifyEvents(fs, hdfsPath, codec, Arrays.asList(
          "multiple-events-1",
          "multiple-events-2",
          "multiple-events-3",
          "multiple-events-4",
          "multiple-events-5"
      ));
    }

    fs.close();
  }

  private void sequenceFileWriteAndVerifyEvents(FileSystem fs, String hdfsPath, String codec,
                                                Collection<String> eventBodies)
      throws IOException, EventDeliveryException {
    Path dirPath = new Path(hdfsPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);

    Context context = new Context();
    context.put("hdfs.path", hdfsPath);
    // Ensure the file isn't closed and rolled
    context.put("hdfs.rollCount", String.valueOf(eventBodies.size() + 1));
    context.put("hdfs.rollSize", "0");
    context.put("hdfs.rollInterval", "0");
    context.put("hdfs.batchSize", "1");
    context.put("hdfs.fileType", "SequenceFile");
    context.put("hdfs.codeC", codec);
    context.put("hdfs.writeFormat", "Writable");
    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    for (String eventBody : eventBodies) {
      Transaction txn = channel.getTransaction();
      txn.begin();

      Event event = new SimpleEvent();
      event.setBody(eventBody.getBytes());
      channel.put(event);

      txn.commit();
      txn.close();

      sink.process();
    }

    // Sink is _not_ closed.  The file should remain open but
    // the data written should be visible to readers via sync + hflush
    FileStatus[] dirStat = fs.listStatus(dirPath);
    Path[] paths = FileUtil.stat2Paths(dirStat);

    Assert.assertEquals(1, paths.length);

    SequenceFile.Reader reader =
        new SequenceFile.Reader(fs.getConf(), SequenceFile.Reader.stream(fs.open(paths[0])));
    LongWritable key = new LongWritable();
    BytesWritable value = new BytesWritable();

    for (String eventBody : eventBodies) {
      Assert.assertTrue(reader.next(key, value));
      Assert.assertArrayEquals(eventBody.getBytes(), value.copyBytes());
    }

    Assert.assertFalse(reader.next(key, value));
  }

  private Context getContextForRetryTests() {
    Context context = new Context();

    context.put("hdfs.path", testPath + "/%{retryHeader}");
    context.put("hdfs.filePrefix", "test");
    context.put("hdfs.batchSize", String.valueOf(100));
    context.put("hdfs.fileType", "DataStream");
    context.put("hdfs.serializer", "text");
    context.put("hdfs.closeTries","3");
    context.put("hdfs.rollCount", "1");
    context.put("hdfs.retryInterval", "1");
    return context;
  }

  @Test
  public void testBadConfigurationForRetryIntervalZero() throws Exception {
    Context context = getContextForRetryTests();
    context.put("hdfs.retryInterval", "0");

    Configurables.configure(sink, context);
    Assert.assertEquals(1, sink.getTryCount());
  }

  @Test
  public void testBadConfigurationForRetryIntervalNegative() throws Exception {
    Context context = getContextForRetryTests();
    context.put("hdfs.retryInterval", "-1");

    Configurables.configure(sink, context);
    Assert.assertEquals(1, sink.getTryCount());
  }

  @Test
  public void testBadConfigurationForRetryCountZero() throws Exception {
    Context context = getContextForRetryTests();
    context.put("hdfs.closeTries" ,"0");

    Configurables.configure(sink, context);
    Assert.assertEquals(Integer.MAX_VALUE, sink.getTryCount());
  }

  @Test
  public void testBadConfigurationForRetryCountNegative() throws Exception {
    Context context = getContextForRetryTests();
    context.put("hdfs.closeTries" ,"-4");

    Configurables.configure(sink, context);
    Assert.assertEquals(Integer.MAX_VALUE, sink.getTryCount());
  }

  @Test
  public void testRetryRename()
      throws InterruptedException, LifecycleException, EventDeliveryException, IOException {
    testRetryRename(true);
    testRetryRename(false);
  }

  private void testRetryRename(boolean closeSucceed)
      throws InterruptedException, LifecycleException, EventDeliveryException, IOException {
    LOG.debug("Starting...");
    String newPath = testPath + "/retryBucket";

    // clear the test directory
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path dirPath = new Path(newPath);
    fs.delete(dirPath, true);
    fs.mkdirs(dirPath);
    MockFileSystem mockFs = new MockFileSystem(fs, 6, closeSucceed);

    Context context = getContextForRetryTests();
    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.setMockFs(mockFs);
    HDFSWriter hdfsWriter = new MockDataStream(mockFs);
    hdfsWriter.configure(context);
    sink.setMockWriter(hdfsWriter);
    sink.start();

    // push the event batches into channel
    for (int i = 0; i < 2; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      Map<String, String> hdr = Maps.newHashMap();
      hdr.put("retryHeader", "v1");

      channel.put(EventBuilder.withBody("random".getBytes(), hdr));
      txn.commit();
      txn.close();

      // execute sink to process the events
      sink.process();
    }
    // push the event batches into channel
    for (int i = 0; i < 2; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      Map<String, String> hdr = Maps.newHashMap();
      hdr.put("retryHeader", "v2");
      channel.put(EventBuilder.withBody("random".getBytes(), hdr));
      txn.commit();
      txn.close();
      // execute sink to process the events
      sink.process();
    }

    TimeUnit.SECONDS.sleep(5); //Sleep till all retries are done.

    Collection<BucketWriter> writers = sink.getSfWriters().values();

    int totalRenameAttempts = 0;
    for (BucketWriter writer : writers) {
      LOG.info("Rename tries = " + writer.renameTries.get());
      totalRenameAttempts += writer.renameTries.get();
    }
    // stop clears the sfWriters map, so we need to compute the
    // close tries count before stopping the sink.
    sink.stop();
    Assert.assertEquals(6, totalRenameAttempts);

  }

  /**
   * BucketWriter.append() can throw a BucketClosedException when called from
   * HDFSEventSink.process() due to a race condition between HDFSEventSink.process() and the
   * BucketWriter's close threads.
   * This test case tests whether if this happens the newly created BucketWriter will be flushed.
   * For more details see FLUME-3085
   */
  @Test
  public void testFlushedIfAppendFailedWithBucketClosedException() throws Exception {
    final Set<BucketWriter> bucketWriters = new HashSet<>();
    sink = new HDFSEventSink() {
      @Override
      BucketWriter initializeBucketWriter(String realPath, String realName, String lookupPath,
                                          HDFSWriter hdfsWriter, WriterCallback closeCallback) {
        BucketWriter bw = Mockito.spy(super.initializeBucketWriter(realPath, realName, lookupPath,
            hdfsWriter, closeCallback));
        try {
          // create mock BucketWriters where the first append() succeeds but the
          // the second call throws a BucketClosedException
          Mockito.doCallRealMethod()
              .doThrow(BucketClosedException.class)
              .when(bw).append(Mockito.any(Event.class));
        } catch (IOException | InterruptedException e) {
          Assert.fail("This shouldn't happen, as append() is called during mocking.");
        }
        bucketWriters.add(bw);
        return bw;
      }
    };

    Context context = new Context(ImmutableMap.of("hdfs.path", testPath));
    Configurables.configure(sink, context);

    Channel channel = Mockito.spy(new MemoryChannel());
    Configurables.configure(channel, new Context());

    final Iterator<Event> events = Iterators.forArray(
        EventBuilder.withBody("test1".getBytes()), EventBuilder.withBody("test2".getBytes()));
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return events.hasNext() ? events.next() : null;
      }
    }).when(channel).take();

    sink.setChannel(channel);
    sink.start();

    sink.process();

    // channel.take() should have called 3 times (2 events + 1 null)
    Mockito.verify(channel, Mockito.times(3)).take();

    FileSystem fs = FileSystem.get(new Configuration());
    int fileCount = 0;
    for (RemoteIterator<LocatedFileStatus> i = fs.listFiles(new Path(testPath), false);
         i.hasNext(); i.next()) {
      fileCount++;
    }
    Assert.assertEquals(2, fileCount);

    Assert.assertEquals(2, bucketWriters.size());
    // It is expected that flush() method was called exactly once for every BucketWriter
    for (BucketWriter bw : bucketWriters) {
      Mockito.verify(bw, Mockito.times(1)).flush();
    }

    sink.stop();
  }
}
