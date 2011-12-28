package org.apache.wal.avro;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestWALIndex {

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
  public void testUpdate() throws FileNotFoundException, IOException {
    index.open();
    Assert.assertNull(index.getFile());
    Assert.assertEquals(0, index.getPosition());

    index.updateIndex("foo", 0);
    Assert.assertEquals("foo", index.getFile());
    Assert.assertEquals(0, index.getPosition());

    index.updateIndex("foo", 1);
    Assert.assertEquals("foo", index.getFile());
    Assert.assertEquals(1, index.getPosition());

    index.updateIndex("foo", 2);
    Assert.assertEquals("foo", index.getFile());
    Assert.assertEquals(2, index.getPosition());

    index.updateIndex("bar", 0);
    Assert.assertEquals("bar", index.getFile());
    Assert.assertEquals(0, index.getPosition());
  }

  @Test
  public void testExistingIndex() throws FileNotFoundException, IOException {
    index.open();
    Assert.assertNull(index.getFile());
    Assert.assertEquals(0, index.getPosition());

    index.updateIndex("test", 128);
    Assert.assertEquals("test", index.getFile());
    Assert.assertEquals(128, index.getPosition());

    index.open();
    Assert.assertEquals("test", index.getFile());
    Assert.assertEquals(128, index.getPosition());
  }

}
