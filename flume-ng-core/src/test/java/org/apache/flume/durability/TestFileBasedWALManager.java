package org.apache.flume.durability;

import java.io.File;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFileBasedWALManager {

  private File testDirectory;
  private FileBasedWALManager walManager;

  @Before
  public void setUp() {
    testDirectory = new File("/tmp/flume-fbwm-tests", String.valueOf(System
        .currentTimeMillis()));
    walManager = new FileBasedWALManager();

    testDirectory.mkdirs();

    walManager.setDirectory(testDirectory);
  }

  @After
  public void tearDown() {
    if (testDirectory.exists()) {
      for (File entry : testDirectory.listFiles()) {
        entry.delete();
      }

      testDirectory.delete();
    }
  }

  @Test
  public void testGetWAL() {
    FileBasedWAL wal = walManager.getWAL("test1");

    Assert.assertNotNull(wal);
    Assert.assertEquals(new File(testDirectory, "test1"),
        wal.getBaseDirectory());
  }

}
