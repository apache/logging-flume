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

}
