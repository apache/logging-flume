package org.apache.wal.avro;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class TestAvroWALWriter {

  private static final Logger logger = LoggerFactory
      .getLogger(TestAvroWALWriter.class);

  private File testDirectory;
  private AvroWALWriter writer;

  @Before
  public void setUp() throws IOException {
    testDirectory = new File("/tmp/wal-avro-" + System.currentTimeMillis());

    testDirectory.mkdirs();

    writer = new AvroWALWriter();
    writer.setDirectory(testDirectory);

    WALIndex index = new WALIndex();

    index.setDirectory(testDirectory);
    index.open();

    writer.setIndex(index);
  }

  @SuppressWarnings("deprecation")
  @After
  public void tearDown() throws IOException {
    Files.deleteRecursively(testDirectory.getCanonicalFile());
  }

  @Test
  public void testWrite() throws IOException {
    writer.open();

    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(
        new FileInputStream(writer.getCurrentFile()), null);
    SpecificDatumReader<AvroWALEntry> reader = new SpecificDatumReader<AvroWALEntry>(
        AvroWALEntry.class);

    for (int i = 1; i <= 205; i++) {
      AvroWALEntryAdapter entry = new AvroWALEntryAdapter(new AvroWALEntry());

      entry.getEntry().setTimeStamp(System.currentTimeMillis());

      writer.write(entry);
      writer.mark();

      AvroWALEntry lastEntry = reader.read(null, decoder);
      logger.debug("read:{}", lastEntry);
      Assert.assertEquals(entry.getEntry(), lastEntry);

      if (i % 100 == 0) {
        logger.debug("Openning a new reader based on writer:{}", writer);

        decoder = DecoderFactory.get().directBinaryDecoder(
            new FileInputStream(writer.getCurrentFile()), null);
      }
    }

    writer.close();
  }

  @Test
  public void testBatchWrite() {
    writer.open();

    File file = writer.getCurrentFile();

    for (int i = 0; i < 150; i++) {
      AvroWALEntryAdapter entry = new AvroWALEntryAdapter(new AvroWALEntry());

      entry.getEntry().setTimeStamp(System.currentTimeMillis());

      writer.write(entry);
    }

    Assert.assertEquals(file, writer.getCurrentFile());

    writer.mark();
    Assert.assertFalse(file.equals(writer.getCurrentFile()));

    writer.close();
  }

}
