package org.apache.flume.channel.file;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFileChannel {

  private static final Logger logger = LoggerFactory
      .getLogger(TestFileChannel.class);

  private FileChannel channel;

  @Before
  public void setUp() {
    channel = new FileChannel();
  }

  @Test(expected = IllegalStateException.class)
  public void testNoDirectory() {
    Event event = EventBuilder.withBody("Test event".getBytes());

    channel.put(event);
  }

  @Test(expected = IllegalStateException.class)
  public void testNonExistantParent() {
    Event event = EventBuilder.withBody("Test event".getBytes());

    channel.setDirectory(new File("/i/do/not/exist"));
    channel.put(event);
  }

  @Test
  public void testGetTransaction() throws IOException {
    File tmpDir = new File("/tmp/flume-fc-test-" + System.currentTimeMillis());
    FileUtils.forceDeleteOnExit(tmpDir);

    channel.setDirectory(tmpDir);

    Transaction tx1 = channel.getTransaction();
    Assert.assertNotNull(tx1);

    Transaction tx2 = channel.getTransaction();
    Assert.assertNotNull(tx2);

    Assert.assertEquals(tx1, tx2);

    tx2.begin();
    Assert.assertEquals(tx2, channel.getTransaction());

    tx2.rollback();
    Assert.assertEquals(tx2, channel.getTransaction());

    tx2.close();
    Assert.assertFalse(tx2.equals(channel.getTransaction()));
  }

  @Test
  public void testPut() throws IOException {
    File tmpDir = new File("/tmp/flume-fc-test-" + System.currentTimeMillis());
    FileUtils.forceDeleteOnExit(tmpDir);

    if (!tmpDir.mkdirs()) {
      throw new IOException("Unable to create test directory:" + tmpDir);
    }

    channel.setDirectory(tmpDir);

    /* Issue five one record transactions. */
    for (int i = 0; i < 5; i++) {
      Transaction transaction = channel.getTransaction();

      Assert.assertNotNull(transaction);

      try {
        transaction.begin();

        Event event = EventBuilder.withBody(("Test event" + i).getBytes());
        channel.put(event);

        transaction.commit();
      } catch (Exception e) {
        logger.error(
            "Failed to put event into file channel. Exception follows.", e);
        transaction.rollback();
        Assert.fail();
      } finally {
        transaction.close();
      }
    }

    /* Issue one five record transaction. */
    Transaction transaction = channel.getTransaction();

    try {
      transaction.begin();

      for (int i = 0; i < 5; i++) {
        Event event = EventBuilder.withBody(("Test event" + i).getBytes());
        channel.put(event);
      }

      transaction.commit();
    } catch (Exception e) {
      logger.error("Failed to put event into file channel. Exception follows.",
          e);
      transaction.rollback();
      Assert.fail();
    } finally {
      transaction.close();
    }
  }

}
