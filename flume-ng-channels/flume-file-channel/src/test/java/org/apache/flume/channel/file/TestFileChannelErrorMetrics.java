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
package org.apache.flume.channel.file;

import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.file.instrumentation.FileChannelCounter;
import org.apache.flume.event.EventBuilder;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;

public class TestFileChannelErrorMetrics extends TestFileChannelBase {

  public TestFileChannelErrorMetrics() {
    // use only 1 data directory in order to make it simpler to edit the data files
    // in testCorruptEventTaken() and testUnhealthy() methods
    super(1);
  }

  /**
   * This tests multiple successful and failed put and take operations
   * and checks the values of the channel's counters.
   */
  @Test
  public void testEventTakePutErrorCount() throws Exception {
    final long usableSpaceRefreshInterval = 1;
    FileChannel channel = Mockito.spy(createFileChannel());
    Mockito.when(channel.createLogBuilder()).then(new Answer<Log.Builder>() {
      @Override
      public Log.Builder answer(InvocationOnMock invocation) throws Throwable {
        Log.Builder ret = (Log.Builder) invocation.callRealMethod();
        ret.setUsableSpaceRefreshInterval(usableSpaceRefreshInterval);
        return ret;
      }
    });
    channel.start();

    FileChannelCounter channelCounter = channel.getChannelCounter();

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test1".getBytes()));
    channel.put(EventBuilder.withBody("test2".getBytes()));
    tx.commit();
    tx.close();
    assertEquals(2, channelCounter.getEventPutAttemptCount());
    assertEquals(2, channelCounter.getEventPutSuccessCount());
    assertEquals(0, channelCounter.getEventPutErrorCount());

    tx = channel.getTransaction();
    tx.begin();
    channel.take();
    tx.commit();
    tx.close();
    assertEquals(1, channelCounter.getEventTakeAttemptCount());
    assertEquals(1, channelCounter.getEventTakeSuccessCount());
    assertEquals(0, channelCounter.getEventTakeErrorCount());

    FileUtils.deleteDirectory(baseDir);
    Thread.sleep(2 * usableSpaceRefreshInterval);

    tx = channel.getTransaction();
    tx.begin();
    ChannelException putException = null;
    try {
      channel.put(EventBuilder.withBody("test".getBytes()));
    } catch (ChannelException ex) {
      putException = ex;
    }
    assertNotNull(putException);
    assertTrue(putException.getCause() instanceof IOException);
    assertEquals(3, channelCounter.getEventPutAttemptCount());
    assertEquals(2, channelCounter.getEventPutSuccessCount());
    assertEquals(1, channelCounter.getEventPutErrorCount());

    ChannelException takeException = null;
    try {
      channel.take(); // This is guaranteed to throw an error if the above put() threw an error.
    } catch (ChannelException ex) {
      takeException = ex;
    }
    assertNotNull(takeException);
    assertTrue(takeException.getCause() instanceof IOException);
    assertEquals(2, channelCounter.getEventTakeAttemptCount());
    assertEquals(1, channelCounter.getEventTakeSuccessCount());
    assertEquals(1, channelCounter.getEventTakeErrorCount());
  }

  /**
   * Test the FileChannelCounter.eventTakeErrorCount value if the data file
   * contains an invalid record thus CorruptEventException is thrown during
   * the take() operation.
   * The first byte of the record (= the first byte of the file in this case)
   * is the operation byte, changing it to an unexpected value will cause the
   * CorruptEventException to be thrown.
   */
  @Test
  public void testCorruptEventTaken() throws Exception {
    FileChannel channel = createFileChannel(
        Collections.singletonMap(FileChannelConfiguration.FSYNC_PER_TXN, "false"));
    channel.start();

    FileChannelCounter channelCounter = channel.getChannelCounter();

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.commit();
    tx.close();

    byte[] data = FileUtils.readFileToByteArray(new File(dataDirs[0], "log-1"));
    data[0] = LogFile.OP_EOF; // change the first (operation) byte to unexpected value
    FileUtils.writeByteArrayToFile(new File(dataDirs[0], "log-1"), data);

    tx = channel.getTransaction();
    tx.begin();

    try {
      channel.take();
    } catch (Throwable t) {
      // If fsyncPerTransaction is false then Log.get throws the CorruptEventException
      // without wrapping it to IOException (which is the case when fsyncPerTransaciton is true)
      // but in this case it is swallowed in FileBackedTransaction.doTake()
      // The eventTakeErrorCount should be increased regardless of this.
      Assert.fail("No exception should be thrown as fsyncPerTransaction is false");
    }

    assertEquals(1, channelCounter.getEventTakeAttemptCount());
    assertEquals(0, channelCounter.getEventTakeSuccessCount());
    assertEquals(1, channelCounter.getEventTakeErrorCount());
  }

  @Test
  public void testCheckpointWriteErrorCount() throws Exception {
    int checkpointInterval = 1500;
    final FileChannel channel = createFileChannel(Collections.singletonMap(
        FileChannelConfiguration.CHECKPOINT_INTERVAL, String.valueOf(checkpointInterval)));
    channel.start();

    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.commit();
    tx.close();

    final long beforeCheckpointWrite = System.currentTimeMillis();

    // first checkpoint should be written successfully -> the counter should remain 0
    assertEventuallyTrue("checkpoint should have been written", new BooleanPredicate() {
      @Override
      public boolean get() {
        return new File(checkpointDir, "checkpoint").lastModified() > beforeCheckpointWrite;
      }
    }, checkpointInterval * 3);
    assertEquals(0, channel.getChannelCounter().getCheckpointWriteErrorCount());

    FileUtils.deleteDirectory(baseDir);

    // the channel's directory has been deleted so the checkpoint write should have been failed
    assertEventuallyTrue("checkpointWriterErrorCount should be 1", new BooleanPredicate() {
      @Override
      public boolean get() {
        return channel.getChannelCounter().getCheckpointWriteErrorCount() == 1;
      }
    }, checkpointInterval * 3);
  }

  /**
   * Test the value of the FileChannelCounter.unhealthy flag after normal startup.
   * It is expected to be 0
   */
  @Test
  public void testHealthy() throws Exception {
    FileChannel channel = createFileChannel();
    assertEquals(0, channel.getChannelCounter().getUnhealthy());
    assertEquals(1, channel.getChannelCounter().getClosed());
    assertFalse(channel.getChannelCounter().isOpen());

    channel.start();
    assertEquals(0, channel.getChannelCounter().getUnhealthy());
    assertEquals(0, channel.getChannelCounter().getClosed());
    assertTrue(channel.getChannelCounter().isOpen());
  }

  /**
   * Test the value of the FileChannelCounter.unhealthy flag after a failed startup.
   * It is expected to be 1
   */
  @Test
  public void testUnhealthy() throws Exception {
    FileChannel channel = createFileChannel();
    assertEquals(0, channel.getChannelCounter().getUnhealthy());
    assertEquals(1, channel.getChannelCounter().getClosed());
    assertFalse(channel.getChannelCounter().isOpen());

    FileUtils.write(new File(dataDirs[0], "log-1"), "invalid data file content");

    channel.start();
    assertEquals(1, channel.getChannelCounter().getUnhealthy());
    assertEquals(1, channel.getChannelCounter().getClosed());
    assertFalse(channel.getChannelCounter().isOpen());
  }

  @Test
  public void testCheckpointBackupWriteErrorShouldIncreaseCounter()
      throws IOException, InterruptedException {
    FileChannelCounter fileChannelCounter = new FileChannelCounter("test");
    File checkpointFile = File.createTempFile("checkpoint", ".tmp");
    File backupDir = Files.createTempDirectory("checkpoint").toFile();
    backupDir.deleteOnExit();
    checkpointFile.deleteOnExit();
    EventQueueBackingStoreFileV3 backingStoreFileV3 = new EventQueueBackingStoreFileV3(
        checkpointFile, 1, "test", fileChannelCounter, backupDir,true, false
    );

    // Exception will be thrown by state check if beforeCheckpoint is not called
    backingStoreFileV3.checkpoint();
    // wait for other thread to reach the error state
    assertEventuallyTrue("checkpoint backup write failure should increase counter to 1",
        new BooleanPredicate() {
          @Override
          public boolean get() {
            return fileChannelCounter.getCheckpointBackupWriteErrorCount() == 1;
          }
        },
        100
    );
  }

  @Test
  public void testCheckpointBackupWriteErrorShouldIncreaseCounter2()
      throws Exception {
    int checkpointInterval = 1500;
    Map config = new HashMap();
    config.put(FileChannelConfiguration.CHECKPOINT_INTERVAL, String.valueOf(checkpointInterval));
    config.put(FileChannelConfiguration.USE_DUAL_CHECKPOINTS, "true");
    final FileChannel channel = createFileChannel(Collections.unmodifiableMap(config));
    channel.start();
    Transaction tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test".getBytes()));
    tx.commit();
    tx.close();
    final long beforeCheckpointWrite = System.currentTimeMillis();
    // first checkpoint should be written successfully -> the counter should remain 0
    assertEventuallyTrue("checkpoint backup should have been written", new BooleanPredicate() {
      @Override
      public boolean get() {
        return new File(backupDir, "checkpoint").lastModified() > beforeCheckpointWrite;
      }
    }, checkpointInterval * 3);
    assertEquals(0, channel.getChannelCounter().getCheckpointBackupWriteErrorCount());
    FileUtils.deleteDirectory(backupDir);
    tx = channel.getTransaction();
    tx.begin();
    channel.put(EventBuilder.withBody("test2".getBytes()));
    tx.commit();
    tx.close();
    // the backup directory has been deleted so the backup checkpoint write should have been failed
    assertEventuallyTrue("checkpointBackupWriteErrorCount should be 1", new BooleanPredicate() {
      @Override
      public boolean get() {
        return channel.getChannelCounter().getCheckpointBackupWriteErrorCount() >= 1;
      }
    }, checkpointInterval * 3);
  }

  private interface BooleanPredicate {
    boolean get();
  }

  private static void assertEventuallyTrue(String description, BooleanPredicate expression,
                                           long timeoutMillis)
      throws InterruptedException {
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() < start + timeoutMillis) {
      if (expression.get()) break;
      Thread.sleep(timeoutMillis / 10);
    }
    assertTrue(description, expression.get());
  }
}
