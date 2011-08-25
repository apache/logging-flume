package org.apache.flume.durability.file;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.formatter.output.TextDelimitedOutputFormatter;
import org.junit.After;
import org.junit.Assert;
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
  public void tearDown() throws IOException {
    FileUtils.deleteDirectory(testDirectory);

    /* Do not die if we can't delete this. Parallel tests could be using it. */
    testDirectory.getParentFile().delete();
  }

  @Test
  public void testWrite() throws IOException {
    testDirectory.mkdirs();

    Event event = new SimpleEvent();

    event.setBody("Test event".getBytes());

    writer.setFormatter(new TextDelimitedOutputFormatter());
    writer.setDirectory(testDirectory);

    writer.open();
    writer.write(event);
    writer.close();

    Assert.assertTrue(writer.getFile().exists());
    Assert.assertTrue(writer.getFile().length() > 0);
  }

}
