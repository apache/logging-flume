package org.apache.wal.avro;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class TestWALIndex {

  private static final Logger logger = LoggerFactory
      .getLogger(TestWALIndex.class);

  private File testDirectory;
  private WALIndex index;

  @Before
  public void setUp() {
    testDirectory = new File("/tmp/wal-avro-index-"
        + System.currentTimeMillis());

    testDirectory.mkdirs();

    index = new WALIndex();
    index.setDirectory(testDirectory);
  }

  @SuppressWarnings("deprecation")
  @After
  public void tearDown() throws IOException {
    Files.deleteRecursively(testDirectory.getCanonicalFile());
  }

  @Test
  public void testUpdateWrite() throws FileNotFoundException, IOException {
    index.open();
    Assert.assertNull(index.getWriteFile());
    Assert.assertEquals(0, index.getWritePosition());

    index.updateWriteIndex("foo", 0);
    Assert.assertEquals("foo", index.getWriteFile());
    Assert.assertEquals(0, index.getWritePosition());

    index.updateWriteIndex("foo", 1);
    Assert.assertEquals("foo", index.getWriteFile());
    Assert.assertEquals(1, index.getWritePosition());

    index.updateWriteIndex("foo", 2);
    Assert.assertEquals("foo", index.getWriteFile());
    Assert.assertEquals(2, index.getWritePosition());

    index.updateWriteIndex("bar", 0);
    Assert.assertEquals("bar", index.getWriteFile());
    Assert.assertEquals(0, index.getWritePosition());
  }

  @Test
  public void testUpdateRead() throws FileNotFoundException, IOException {
    index.open();
    Assert.assertNull(index.getReadFile());
    Assert.assertEquals(0, index.getReadPosition());

    index.updateReadIndex("foo", 0);
    Assert.assertEquals("foo", index.getReadFile());
    Assert.assertEquals(0, index.getReadPosition());

    index.updateReadIndex("foo", 1);
    Assert.assertEquals("foo", index.getReadFile());
    Assert.assertEquals(1, index.getReadPosition());

    index.updateReadIndex("foo", 2);
    Assert.assertEquals("foo", index.getReadFile());
    Assert.assertEquals(2, index.getReadPosition());

    index.updateReadIndex("bar", 0);
    Assert.assertEquals("bar", index.getReadFile());
    Assert.assertEquals(0, index.getReadPosition());
  }

  @Test
  public void testExistingIndex() throws FileNotFoundException, IOException {
    index.open();
    Assert.assertNull(index.getWriteFile());
    Assert.assertEquals(0, index.getWritePosition());
    Assert.assertNull(index.getReadFile());
    Assert.assertEquals(0, index.getReadPosition());

    index.updateWriteIndex("test1", 1);
    index.updateReadIndex("test2", 2);
    Assert.assertEquals("test1", index.getWriteFile());
    Assert.assertEquals(1, index.getWritePosition());
    Assert.assertEquals("test2", index.getReadFile());
    Assert.assertEquals(2, index.getReadPosition());

    index.open();
    Assert.assertEquals("test1", index.getWriteFile());
    Assert.assertEquals(1, index.getWritePosition());
    Assert.assertEquals("test2", index.getReadFile());
    Assert.assertEquals(2, index.getReadPosition());
  }

  @Test
  public void testBruteForceDeadlockDetect() throws FileNotFoundException,
      IOException, InterruptedException {

    index.open();

    ExecutorService executor = Executors.newFixedThreadPool(8);
    final Random random = new Random();

    for (int i = 0; i < 50000; i++) {
      final long count = i;

      executor.submit(new Runnable() {

        @Override
        public void run() {
          if (count % 1000 == 0) {
            logger.debug("count:{}", count);
          }

          if (random.nextBoolean()) {
            index.updateReadIndex("a", count);
          } else {
            index.updateWriteIndex("a", count);
          }
        }
      });
    }

    executor.shutdown();

    while (!executor.isTerminated()) {
      executor.awaitTermination(1, TimeUnit.SECONDS);
    }
  }

}
