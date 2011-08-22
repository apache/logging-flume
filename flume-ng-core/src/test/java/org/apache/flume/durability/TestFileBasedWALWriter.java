package org.apache.flume.durability;

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.flume.Event;
import org.apache.flume.durability.FileBasedWALManager.FileBasedWALWriter;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.formatter.output.TextDelimitedOutputFormatter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFileBasedWALWriter {

  private File testDirectory;
  private FileBasedWALWriter writer;

  @Before
  public void setUp() {
    testDirectory = new File("/tmp/flume-fbww-tests", String.valueOf(System
        .currentTimeMillis()));
    writer = new FileBasedWALWriter();
  }

  @After
  public void tearDown() {
    if (testDirectory.exists()) {
      for (File entry : testDirectory.listFiles()) {
        entry.delete();
      }

      testDirectory.delete();
    }

    testDirectory.getParentFile().delete();
  }

  @Test
  public void testWrite() throws IOException {
    testDirectory.mkdirs();

    Event event = new SimpleEvent();

    event.setBody("Test event".getBytes());

    writer.setFormatter(new TextDelimitedOutputFormatter());
    writer.setFile(new File(testDirectory, "test.wal"));

    writer.open();
    writer.write(event);
    writer.close();

    Assert.assertTrue(writer.getFile().exists());
    Assert.assertTrue(writer.getFile().length() > 0);
  }

}
