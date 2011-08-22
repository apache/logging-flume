package org.apache.flume.durability.file;

import java.io.File;
import java.io.IOException;

import org.apache.flume.durability.file.FileBasedWAL;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestFileBasedWAL {

  private File testBaseDirectory;
  private FileBasedWAL wal;

  @Before
  public void setUp() {
    testBaseDirectory = new File("/tmp/flume-fbw-tests/"
        + System.currentTimeMillis() + "-" + Thread.currentThread().getId());

    wal = new FileBasedWAL(testBaseDirectory);
  }

  @After
  public void tearDown() throws IOException {
    /*
     * Google's Files.deleteRecursively(File) bails out if anything in the path
     * is a symlink. On MOSX, /tmp is a symlink to /private/tmp so it breaks.
     */
    if (testBaseDirectory.exists()) {
      for (File file : testBaseDirectory.listFiles()) {
        file.delete();
      }

      testBaseDirectory.delete();
    }

    /*
     * Attempt to delete the parent, but be happy with failure. This should
     * allow parallel tests to occur.
     */

    testBaseDirectory.getParentFile().delete();
  }

  @Test
  public void testInitialize() throws IOException {
    testBaseDirectory.mkdirs();

    wal.initialize();

    Assert.assertTrue("WAL test directory doesn't exist:" + testBaseDirectory,
        testBaseDirectory.exists());

    Assert.assertTrue("Directory doesn't exist:" + wal.getOpenDirectory(), wal
        .getOpenDirectory().exists());
    Assert.assertTrue("Directory doesn't exist:" + wal.getPendingDirectory(),
        wal.getPendingDirectory().exists());
    Assert.assertTrue("Directory doesn't exist:" + wal.getSentDirectory(), wal
        .getSentDirectory().exists());
    Assert.assertTrue("Directory doesn't exist:" + wal.getCompleteDirectory(),
        wal.getCompleteDirectory().exists());

  }

  @Test
  public void testDirectories() throws IOException {
    testBaseDirectory.mkdirs();

    wal.initialize();

    Assert.assertEquals(testBaseDirectory, wal.getBaseDirectory());
    Assert.assertEquals(new File(testBaseDirectory, "open"),
        wal.getOpenDirectory());
    Assert.assertEquals(new File(testBaseDirectory, "pending"),
        wal.getPendingDirectory());
    Assert.assertEquals(new File(testBaseDirectory, "sent"),
        wal.getSentDirectory());
    Assert.assertEquals(new File(testBaseDirectory, "complete"),
        wal.getCompleteDirectory());
  }
}
