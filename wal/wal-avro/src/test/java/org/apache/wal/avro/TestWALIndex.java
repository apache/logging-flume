package org.apache.wal.avro;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

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

    index.updateIndex("foo", 0);
    index.updateIndex("foo", 1);
    index.updateIndex("foo", 2);
    index.updateIndex("bar", 0);
  }

}
