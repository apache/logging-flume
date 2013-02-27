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
import java.util.List;
import java.util.UUID;

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
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;


public class TestHDFSEventSink {

  private HDFSEventSink sink;
  private String testPath;
  private static final Logger LOG = LoggerFactory
      .getLogger(HDFSEventSink.class);

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
    if (System.getenv("hdfs_keepFiles") == null)
      dirCleanup();
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
    for (i = 1; i <= (rollCount*10)/batchSize; i++) {
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
    Path fList[] = FileUtil.stat2Paths(dirStat);

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
          "is nonexistent or can't read."));
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
    Path fList[] = FileUtil.stat2Paths(dirStat);

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
    Path fList[] = FileUtil.stat2Paths(dirStat);

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
    Path fList[] = FileUtil.stat2Paths(dirStat);

    // check that the roll happened correctly for the given data
    long expectedFiles = totalEvents / rollCount;
    if (totalEvents % rollCount > 0) expectedFiles++;
    Assert.assertEquals("num files wrong, found: " +
        Lists.newArrayList(fList), expectedFiles, fList.length);
    verifyOutputSequenceFiles(fs, conf, dirPath.toUri().getPath(), fileName, bodies);
  }

  @Test
  public void testSimpleAppendLocalTime() throws InterruptedException,
    LifecycleException, EventDeliveryException, IOException {
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
      String.valueOf(currentTime/1000);
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
    Path fList[] = FileUtil.stat2Paths(dirStat);

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

    HDFSBadWriterFactory badWriterFactory = new HDFSBadWriterFactory();
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
    context.put("hdfs.fileType", HDFSBadWriterFactory.BadSequenceFileType);

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
    if(dir.isFile()) {
      output.add(dir.getAbsolutePath());
    } else if(dir.isDirectory()) {
      for(String file : dir.list()) {
        File subDir = new File(dir, file);
        output.addAll(getAllFiles(subDir.getAbsolutePath()));
      }
    }
    return output;
  }

  private void verifyOutputSequenceFiles(FileSystem fs, Configuration conf, String dir, String prefix, List<String> bodies) throws IOException {
    int found = 0;
    int expected = bodies.size();
    for(String outputFile : getAllFiles(dir)) {
      String name = (new File(outputFile)).getName();
      if(name.startsWith(prefix)) {
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(outputFile), conf);
        LongWritable key = new LongWritable();
        BytesWritable value = new BytesWritable();
        while(reader.next(key, value)) {
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

  private void verifyOutputTextFiles(FileSystem fs, Configuration conf, String dir, String prefix, List<String> bodies) throws IOException {
    int found = 0;
    int expected = bodies.size();
    for(String outputFile : getAllFiles(dir)) {
      String name = (new File(outputFile)).getName();
      if(name.startsWith(prefix)) {
        FSDataInputStream input = fs.open(new Path(outputFile));
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String body = null;
        while((body = reader.readLine()) != null) {
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

  private void verifyOutputAvroFiles(FileSystem fs, Configuration conf, String dir, String prefix, List<String> bodies) throws IOException {
    int found = 0;
    int expected = bodies.size();
    for(String outputFile : getAllFiles(dir)) {
      String name = (new File(outputFile)).getName();
      if(name.startsWith(prefix)) {
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
  public void testCloseReopen() throws InterruptedException,
      LifecycleException, EventDeliveryException, IOException {

    LOG.debug("Starting...");
    final int numBatches = 4;
    final String fileName = "FlumeData";
    final long rollCount = 5;
    final long batchSize = 2;
    String newPath = testPath + "/singleBucket";
    int i = 1, j = 1;

    HDFSBadWriterFactory badWriterFactory = new HDFSBadWriterFactory();
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
    context.put("hdfs.fileType", HDFSBadWriterFactory.BadSequenceFileType);

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
    HDFSBadWriterFactory badWriterFactory = new HDFSBadWriterFactory();
    sink = new HDFSEventSink(badWriterFactory);

    Context context = new Context();
    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.fileType", HDFSBadWriterFactory.BadSequenceFileType);
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
  private void slowAppendTestHelper (long appendTimeout)  throws InterruptedException, IOException,
  LifecycleException, EventDeliveryException, IOException {
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
    HDFSBadWriterFactory badWriterFactory = new HDFSBadWriterFactory();
    sink = new HDFSEventSink(badWriterFactory);

    Context context = new Context();
    context.put("hdfs.path", newPath);
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.fileType", HDFSBadWriterFactory.BadSequenceFileType);
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
    Path fList[] = FileUtil.stat2Paths(dirStat);

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
    for(int i=0; i < 10; i++) {
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
    // this can throw an IOException(from the bucketWriter having idleClosed)
    // this is not an issue as the sink will retry and get a fresh bucketWriter
    // so long as the onIdleClose handler properly removes bucket writers that
    // were closed due to idling
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
}
