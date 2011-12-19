package org.apache.wal.avro;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.wal.WALEntry;
import org.apache.wal.avro.AvroWALEntry;
import org.apache.wal.avro.AvroWALEntryAdapter;
import org.apache.wal.avro.AvroWALReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestAvroWALReader {

  private static final Logger logger = LoggerFactory
      .getLogger(TestAvroWALReader.class);

  private File testDirectory;
  private AvroWALReader reader;

  @Before
  public void setUp() throws IOException {
    testDirectory = new File("/tmp/wal-avro-" + System.currentTimeMillis());

    testDirectory.mkdirs();

    reader = new AvroWALReader();

    FileInputStream walInputStream = new FileInputStream(new File(Resources
        .getResource("test-wal").getFile()));

    MappedByteBuffer indexBuffer = Files.map(new File(testDirectory, "index"),
        FileChannel.MapMode.READ_WRITE, 8);
    indexBuffer.putLong(0, 0);

    reader.setDecoder(DecoderFactory.get().directBinaryDecoder(walInputStream,
        null));
    reader.setAvroReader(new SpecificDatumReader<AvroWALEntry>(
        AvroWALEntry.SCHEMA$));
    reader.setWalInputStream(walInputStream);
    reader.setIndexBuffer(indexBuffer);
  }

  @SuppressWarnings("deprecation")
  @After
  public void tearDown() throws IOException {
    Files.deleteRecursively(testDirectory.getCanonicalFile());
  }

  @Test
  public void testNext() {
    for (int i = 0; i < 184; i++) {
      WALEntry entry = reader.next();

      logger.debug("Read:{}", entry);

      Assert.assertNotNull(entry);
      Assert.assertTrue(entry instanceof AvroWALEntryAdapter);
      Assert.assertNotNull(((AvroWALEntryAdapter) entry).getEntry());

      reader.mark();
    }
  }

}
