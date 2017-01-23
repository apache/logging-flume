/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flume.source;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestSpoolDirectorySource {
  static SpoolDirectorySource source;
  static MemoryChannel channel;
  private File tmpDir;

  @Before
  public void setUp() {
    source = new SpoolDirectorySource();
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
    tmpDir = Files.createTempDir();
  }

  @After
  public void tearDown() {
    deleteFiles(tmpDir);
    tmpDir.delete();
  }

  /**
   * Helper method to recursively clean up testing directory
   *
   * @param directory the directory to clean up
   */
  private void deleteFiles(File directory) {
    for (File f : directory.listFiles()) {
      if (f.isDirectory()) {
        deleteFiles(f);
        f.delete();
      } else {
        f.delete();
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidSortOrder() {
    Context context = new Context();
    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
        tmpDir.getAbsolutePath());
    context.put(SpoolDirectorySourceConfigurationConstants.CONSUME_ORDER,
        "undefined");
    Configurables.configure(source, context);
  }

  @Test
  public void testValidSortOrder() {
    Context context = new Context();
    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
        tmpDir.getAbsolutePath());
    context.put(SpoolDirectorySourceConfigurationConstants.CONSUME_ORDER,
        "oLdESt");
    Configurables.configure(source, context);
    context.put(SpoolDirectorySourceConfigurationConstants.CONSUME_ORDER,
        "yoUnGest");
    Configurables.configure(source, context);
    context.put(SpoolDirectorySourceConfigurationConstants.CONSUME_ORDER,
        "rAnDom");
    Configurables.configure(source, context);
  }

  @Test
  public void testPutFilenameHeader() throws IOException, InterruptedException {
    Context context = new Context();
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
        tmpDir.getAbsolutePath());
    context.put(SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER,
        "true");
    context.put(SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER_KEY,
        "fileHeaderKeyTest");

    Configurables.configure(source, context);
    source.start();
    while (source.getSourceCounter().getEventAcceptedCount() < 8) {
      Thread.sleep(10);
    }
    Transaction txn = channel.getTransaction();
    txn.begin();
    Event e = channel.take();
    Assert.assertNotNull("Event must not be null", e);
    Assert.assertNotNull("Event headers must not be null", e.getHeaders());
    Assert.assertNotNull(e.getHeaders().get("fileHeaderKeyTest"));
    Assert.assertEquals(f1.getAbsolutePath(),
        e.getHeaders().get("fileHeaderKeyTest"));
    txn.commit();
    txn.close();
  }

  /**
   * Tests if SpoolDirectorySource sets basename headers on events correctly
   */
  @Test
  public void testPutBasenameHeader() throws IOException, InterruptedException {
    Context context = new Context();
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                f1, Charsets.UTF_8);

    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
        tmpDir.getAbsolutePath());
    context.put(SpoolDirectorySourceConfigurationConstants.BASENAME_HEADER,
        "true");
    context.put(SpoolDirectorySourceConfigurationConstants.BASENAME_HEADER_KEY,
        "basenameHeaderKeyTest");

    Configurables.configure(source, context);
    source.start();
    while (source.getSourceCounter().getEventAcceptedCount() < 8) {
      Thread.sleep(10);
    }
    Transaction txn = channel.getTransaction();
    txn.begin();
    Event e = channel.take();
    Assert.assertNotNull("Event must not be null", e);
    Assert.assertNotNull("Event headers must not be null", e.getHeaders());
    Assert.assertNotNull(e.getHeaders().get("basenameHeaderKeyTest"));
    Assert.assertEquals(f1.getName(),
        e.getHeaders().get("basenameHeaderKeyTest"));
    txn.commit();
    txn.close();
  }

  /**
   * Tests SpoolDirectorySource with parameter recursion set to true
   */
  @Test
  public void testRecursion_SetToTrue() throws IOException, InterruptedException {
    File subDir = new File(tmpDir, "directorya/directoryb/directoryc");
    boolean directoriesCreated = subDir.mkdirs();
    Assert.assertTrue("source directories must be created", directoriesCreated);

    final String FILE_NAME = "recursion_file.txt";
    File f1 = new File(subDir, FILE_NAME);
    String origBody = "file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
        "file1line5\nfile1line6\nfile1line7\nfile1line8\n";
    Files.write(origBody, f1, Charsets.UTF_8);

    Context context = new Context();
    context.put(SpoolDirectorySourceConfigurationConstants.RECURSIVE_DIRECTORY_SEARCH,
        "true"); // enable recursion, so we should find the file we created above
    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
        tmpDir.getAbsolutePath()); // spool set to root dir
    context.put(SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER,
        "true"); // put the file name in the "file" header

    Configurables.configure(source, context);
    source.start();
    Assert.assertTrue("Recursion setting in source is correct",
        source.getRecursiveDirectorySearch());


    Transaction txn = channel.getTransaction();
    txn.begin();
    long startTime = System.currentTimeMillis();
    Event e = null;
    while (System.currentTimeMillis() - startTime < 300 && e == null) {
      e = channel.take();
      Thread.sleep(10);
    }

    Assert.assertNotNull("Event must not be null", e);

    Assert.assertNotNull("Event headers must not be null", e.getHeaders());
    Assert.assertTrue("File header value did not end with expected filename",
        e.getHeaders().get("file").endsWith(FILE_NAME));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    do { // collecting the whole body
      baos.write(e.getBody());
      baos.write('\n'); // newline characters are consumed in the process
      e = channel.take();
    } while (e != null);

    Assert.assertEquals("Event body is correct",
        Arrays.toString(origBody.getBytes()),
        Arrays.toString(baos.toByteArray()));
    txn.commit();
    txn.close();
  }


  /**
   * This test will place a file into a sub-directory of the spool directory
   * since the recursion setting is false there should not be any transactions
   * to take from the channel.  The 500 ms is arbitrary and simply follows
   * what the other tests use to "assume" that since there is no data then this worked.
   */
  @Test
  public void testRecursion_SetToFalse() throws IOException, InterruptedException {
    Context context = new Context();

    File subDir = new File(tmpDir, "directory");
    boolean directoriesCreated = subDir.mkdirs();
    Assert.assertTrue("source directories must be created", directoriesCreated);

    File f1 = new File(subDir.getAbsolutePath() + "/file1.txt");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
        "file1line5\nfile1line6\nfile1line7\nfile1line8\n", f1, Charsets.UTF_8);

    context.put(SpoolDirectorySourceConfigurationConstants.RECURSIVE_DIRECTORY_SEARCH,
        "false");
    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
        tmpDir.getAbsolutePath());
    context.put(SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER,
        "true");
    context.put(SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER_KEY,
        "fileHeaderKeyTest");

    Configurables.configure(source, context);
    source.start();
    // check the source to ensure the setting has been set via the context object
    Assert.assertFalse("Recursion setting in source is not set to false (this" +
        "test does not want recursion enabled)", source.getRecursiveDirectorySearch());

    Transaction txn = channel.getTransaction();
    txn.begin();
    long startTime = System.currentTimeMillis();
    Event e = null;
    while (System.currentTimeMillis() - startTime < 300 && e == null) {
      e = channel.take();
      Thread.sleep(10);
    }
    Assert.assertNull("Event must be null", e);
    txn.commit();
    txn.close();
  }

  @Test
  public void testLifecycle() throws IOException, InterruptedException {
    Context context = new Context();
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
        "file1line5\nfile1line6\nfile1line7\nfile1line8\n", f1, Charsets.UTF_8);

    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
        tmpDir.getAbsolutePath());

    Configurables.configure(source, context);

    for (int i = 0; i < 10; i++) {
      source.start();

      Assert
          .assertTrue("Reached start or error", LifecycleController.waitForOneOf(
              source, LifecycleState.START_OR_ERROR));
      Assert.assertEquals("Server is started", LifecycleState.START,
          source.getLifecycleState());

      source.stop();
      Assert.assertTrue("Reached stop or error",
          LifecycleController.waitForOneOf(source, LifecycleState.STOP_OR_ERROR));
      Assert.assertEquals("Server is stopped", LifecycleState.STOP,
          source.getLifecycleState());
    }
  }

  @Test
  public void testReconfigure() throws InterruptedException, IOException {
    final int NUM_RECONFIGS = 20;
    for (int i = 0; i < NUM_RECONFIGS; i++) {
      Context context = new Context();
      File file = new File(tmpDir.getAbsolutePath() + "/file-" + i);
      Files.write("File " + i, file, Charsets.UTF_8);
      context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
          tmpDir.getAbsolutePath());
      Configurables.configure(source, context);
      source.start();
      Thread.sleep(TimeUnit.SECONDS.toMillis(1));
      Transaction txn = channel.getTransaction();
      txn.begin();
      try {
        Event event = channel.take();
        String content = new String(event.getBody(), Charsets.UTF_8);
        Assert.assertEquals("File " + i, content);
        txn.commit();
      } catch (Throwable t) {
        txn.rollback();
      } finally {
        txn.close();
      }
      source.stop();
      Assert.assertFalse("Fatal error on iteration " + i, source.hasFatalError());
    }
  }

  @Test
  public void testSourceDoesNotDieOnFullChannel() throws Exception {

    Context chContext = new Context();
    chContext.put("capacity", "2");
    chContext.put("transactionCapacity", "2");
    chContext.put("keep-alive", "0");
    channel.stop();
    Configurables.configure(channel, chContext);

    channel.start();
    Context context = new Context();
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\nfile1line2\nfile1line3\nfile1line4\n" +
                "file1line5\nfile1line6\nfile1line7\nfile1line8\n",
                 f1, Charsets.UTF_8);

    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
        tmpDir.getAbsolutePath());

    context.put(SpoolDirectorySourceConfigurationConstants.BATCH_SIZE, "2");
    Configurables.configure(source, context);
    source.setBackOff(false);
    source.start();

    // Wait for the source to read enough events to fill up the channel.

    long startTime = System.currentTimeMillis();
    while (System.currentTimeMillis() - startTime < 5000 && !source.didHitChannelFullException()) {
      Thread.sleep(10);
    }

    Assert.assertTrue("Expected to hit ChannelFullException, but did not!",
                      source.didHitChannelFullException());

    List<String> dataOut = Lists.newArrayList();

    for (int i = 0; i < 8; ) {
      Transaction tx = channel.getTransaction();
      tx.begin();
      Event e = channel.take();
      if (e != null) {
        dataOut.add(new String(e.getBody(), "UTF-8"));
        i++;
      }
      e = channel.take();
      if (e != null) {
        dataOut.add(new String(e.getBody(), "UTF-8"));
        i++;
      }
      tx.commit();
      tx.close();
    }
    Assert.assertEquals(8, dataOut.size());
    source.stop();
  }

  @Test
  public void testEndWithZeroByteFiles() throws IOException, InterruptedException {
    Context context = new Context();

    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");

    Files.write("file1line1\n", f1, Charsets.UTF_8);

    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    File f3 = new File(tmpDir.getAbsolutePath() + "/file3");
    File f4 = new File(tmpDir.getAbsolutePath() + "/file4");

    Files.touch(f2);
    Files.touch(f3);
    Files.touch(f4);

    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
        tmpDir.getAbsolutePath());
    Configurables.configure(source, context);
    source.start();

    // Need better way to ensure all files were processed.
    Thread.sleep(5000);

    Assert.assertFalse("Server did not error", source.hasFatalError());
    Assert.assertEquals("Four messages were read",
        4, source.getSourceCounter().getEventAcceptedCount());
    source.stop();
  }

  @Test
  public void testWithAllEmptyFiles()
      throws InterruptedException, IOException {
    Context context = new Context();
    File[] f = new File[10];
    for (int i = 0; i < 10; i++) {
      f[i] = new File(tmpDir.getAbsolutePath() + "/file" + i);
      Files.write(new byte[0], f[i]);
    }
    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
        tmpDir.getAbsolutePath());
    context.put(SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER,
        "true");
    context.put(SpoolDirectorySourceConfigurationConstants.FILENAME_HEADER_KEY,
        "fileHeaderKeyTest");
    Configurables.configure(source, context);
    source.start();
    Thread.sleep(10);
    for (int i = 0; i < 10; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      Event e = channel.take();
      Assert.assertNotNull("Event must not be null", e);
      Assert.assertNotNull("Event headers must not be null", e.getHeaders());
      Assert.assertNotNull(e.getHeaders().get("fileHeaderKeyTest"));
      Assert.assertEquals(f[i].getAbsolutePath(),
          e.getHeaders().get("fileHeaderKeyTest"));
      Assert.assertArrayEquals(new byte[0], e.getBody());
      txn.commit();
      txn.close();
    }
    source.stop();
  }

  @Test
  public void testWithEmptyAndDataFiles()
      throws InterruptedException, IOException {
    Context context = new Context();
    File f1 = new File(tmpDir.getAbsolutePath() + "/file1");
    Files.write("some data".getBytes(), f1);
    File f2 = new File(tmpDir.getAbsolutePath() + "/file2");
    Files.write(new byte[0], f2);
    context.put(SpoolDirectorySourceConfigurationConstants.SPOOL_DIRECTORY,
        tmpDir.getAbsolutePath());
    Configurables.configure(source, context);
    source.start();
    Thread.sleep(10);
    for (int i = 0; i < 2; i++) {
      Transaction txn = channel.getTransaction();
      txn.begin();
      Event e = channel.take();
      txn.commit();
      txn.close();
    }
    Transaction txn = channel.getTransaction();
    txn.begin();
    Assert.assertNull(channel.take());
    txn.commit();
    txn.close();
    source.stop();
  }
}
