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

import java.io.IOException;
import java.util.Calendar;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.sink.hdfs.HDFSEventSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHDFSEventSink {

  private HDFSEventSink sink;
  private String testPath;

  @Before
  public void setUp() {
    /*
     * FIXME: Use a dynamic path to support concurrent test execution. Also,
     * beware of the case where this path is used for something or when the
     * Hadoop config points at file:/// rather than hdfs://. We need to find a
     * better way of testing HDFS related functionality.
     */
    testPath = "/user/flume/testdata";
    sink = new HDFSEventSink();
  }

  @After
  public void tearDown() {
  }

  @Test
  public void testLifecycle() throws InterruptedException, LifecycleException {
    Context context = new Context();

    context.put("hdfs.path", testPath);
    /*
     * context.put("hdfs.rollInterval", String.class);
     * context.get("hdfs.rollSize", String.class); context.get("hdfs.rollCount",
     * String.class);
     */
    Configurables.configure(sink, context);

    sink.start();
    sink.stop();
  }

  @Test
  public void testAppend() throws InterruptedException, LifecycleException,
      EventDeliveryException, IOException {

    final long txnMax = 25;
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
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.txnEventMax", String.valueOf(txnMax));
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));

    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    Transaction txn = channel.getTransaction();

    Calendar eventDate = Calendar.getInstance();

    // push the event batches into channel
    for (int i = 1; i < 4; i++) {
      txn.begin();
      for (int j = 1; j <= txnMax; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);

        event.setBody(("Test." + i + "." + j).getBytes());
        channel.put(event);
      }
      txn.commit();

      // execute sink to process the events
      sink.process();
    }

    sink.stop();

    /*
     * 
     * // loop through all the files generated and check their contains
     * FileStatus[] dirStat = fs.listStatus(dirPath); Path fList[] =
     * FileUtil.stat2Paths(dirStat);
     * 
     * try { for (int cnt = 0; cnt < fList.length; cnt++) { SequenceFile.Reader
     * reader = new SequenceFile.Reader(fs, fList[cnt], conf); LongWritable key
     * = new LongWritable(); BytesWritable value = new BytesWritable();
     * 
     * while (reader.next(key, value)) { logger.info(key+ ":" +
     * value.toString()); } reader.close(); } } catch (IOException ioe) {
     * System.err.println("IOException during operation: " + ioe.toString());
     * System.exit(1); }
     */
  }

  @Test
  public void testTextAppend() throws InterruptedException, LifecycleException,
      EventDeliveryException, IOException {

    final long txnMax = 25;
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
    context.put("hdfs.filePrefix", fileName);
    context.put("hdfs.txnEventMax", String.valueOf(txnMax));
    context.put("hdfs.rollCount", String.valueOf(rollCount));
    context.put("hdfs.batchSize", String.valueOf(batchSize));
    context.put("hdfs.writeFormat","Text");
    context.put("hdfs.fileType", "DataStream");

    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, context);

    sink.setChannel(channel);
    sink.start();

    Transaction txn = channel.getTransaction();

    Calendar eventDate = Calendar.getInstance();

    // push the event batches into channel
    for (int i = 1; i < 4; i++) {
      txn.begin();
      for (int j = 1; j <= txnMax; j++) {
        Event event = new SimpleEvent();
        eventDate.clear();
        eventDate.set(2011, i, i, i, 0); // yy mm dd
        event.getHeaders().put("timestamp",
            String.valueOf(eventDate.getTimeInMillis()));
        event.getHeaders().put("hostname", "Host" + i);

        event.setBody(("Test." + i + "." + j).getBytes());
        channel.put(event);
      }
      txn.commit();

      // execute sink to process the events
      sink.process();
    }

    sink.stop();

    /*
     * 
     * // loop through all the files generated and check their contains
     * FileStatus[] dirStat = fs.listStatus(dirPath); Path fList[] =
     * FileUtil.stat2Paths(dirStat);
     * 
     * try { for (int cnt = 0; cnt < fList.length; cnt++) { SequenceFile.Reader
     * reader = new SequenceFile.Reader(fs, fList[cnt], conf); LongWritable key
     * = new LongWritable(); BytesWritable value = new BytesWritable();
     * 
     * while (reader.next(key, value)) { logger.info(key+ ":" +
     * value.toString()); } reader.close(); } } catch (IOException ioe) {
     * System.err.println("IOException during operation: " + ioe.toString());
     * System.exit(1); }
     */
  }

  
}
