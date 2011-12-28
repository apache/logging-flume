package org.apache.wal.avro;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.wal.WALEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;

public class TestAvroWAL {

  private File testDirectory;
  private AvroWAL wal;

  @Before
  public void setUp() {
    testDirectory = new File("/tmp/wal-avro-" + System.currentTimeMillis());

    testDirectory.mkdirs();

    wal = new AvroWAL();

    Map<String, String> conf = new HashMap<String, String>();

    conf.put("directory", testDirectory.getPath());
    wal.configure(conf);
  }

  @After
  public void tearDown() throws IOException {
    Files.deleteRecursively(testDirectory.getCanonicalFile());
  }

  @Test
  public void testReadWrite() throws InterruptedException {
    wal.open();

    final AvroWALReader reader = (AvroWALReader) wal.getReader();
    final AvroWALWriter writer = (AvroWALWriter) wal.getWriter();

    Runnable readerRunner = new Runnable() {

      @Override
      public void run() {
        reader.open();

        for (int i = 0; i < 5000; i++) {
          WALEntry entry = reader.next();

          if (i % 1000 == 0) {
            reader.mark();
          }

          if (entry == null) {
            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }

        }

        reader.mark();
        reader.close();
      }

    };

    Runnable writerRunner = new Runnable() {

      @Override
      public void run() {
        writer.open();

        for (int i = 0; i < 5000; i++) {
          AvroWALEntryAdapter entry = new AvroWALEntryAdapter(
              new AvroWALEntry());

          entry.getEntry().setTimeStamp((long) i);

          writer.write(entry);

          if (i % 100 == 0) {
            writer.mark();
          }
        }

        writer.mark();
        writer.close();
      }

    };

    ExecutorService executor = Executors.newFixedThreadPool(2);

    executor.submit(writerRunner);
    executor.submit(readerRunner);

    executor.shutdown();

    while (!executor.isTerminated()) {
      executor.awaitTermination(1, TimeUnit.SECONDS);
    }

    wal.close();
  }
}
