package org.apache.wal.avro;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import junit.framework.Assert;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.wal.avro.AvroWALEntry;
import org.apache.wal.avro.AvroWALEntryAdapter;
import org.apache.wal.avro.AvroWALWriter;
import org.junit.After;
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

    MappedByteBuffer indexBuffer = Files.map(new File(testDirectory,
        "write-index"), FileChannel.MapMode.READ_WRITE, 8);
    FileOutputStream walOutputStream = new FileOutputStream(new File(
        testDirectory, "wal"), true);
    Encoder encoder = EncoderFactory.get().directBinaryEncoder(walOutputStream,
        null);
    SpecificDatumWriter<AvroWALEntry> avroWriter = new SpecificDatumWriter<AvroWALEntry>(
        AvroWALEntry.class);

    writer = new AvroWALWriter();

    writer.setEncoder(encoder);
    writer.setIndexBuffer(indexBuffer);
    writer.setWalOutputStream(walOutputStream);
    writer.setWriter(avroWriter);
  }

  @SuppressWarnings("deprecation")
  @After
  public void tearDown() throws IOException {
    Files.deleteRecursively(testDirectory.getCanonicalFile());
  }

  @Test
  public void testWrite() throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(
        new FileInputStream(new File(testDirectory, "wal")), null);
    SpecificDatumReader<AvroWALEntry> reader = new SpecificDatumReader<AvroWALEntry>(
        AvroWALEntry.class);

    writer.open();

    for (int i = 0; i < 205; i++) {
      AvroWALEntryAdapter entry = new AvroWALEntryAdapter(new AvroWALEntry());

      entry.getEntry().timeStamp = System.currentTimeMillis();

      writer.write(entry);

      if (i % 10 == 0) {
        writer.reset();
      } else {
        writer.mark();
      }

      try {
        AvroWALEntry lastEntry = reader.read(null, decoder);
        logger.debug("read:{}", lastEntry);
      } catch (EOFException e) {
        Assert.assertTrue(i % 10 == 0);
      }

    }

    writer.mark();
    writer.close();
  }
}
