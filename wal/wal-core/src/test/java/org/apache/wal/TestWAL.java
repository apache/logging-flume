package org.apache.wal;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.wal.TestWAL.MockWAL.MockWALEntry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestWAL {

  private static final Logger logger = LoggerFactory.getLogger(TestWAL.class);

  private WAL wal;

  @Before
  public void setUp() {
    wal = new MockWAL();
  }

  @Test
  public void testLifecycle() {
    Map<String, String> properties = new HashMap<String, String>();

    wal.configure(properties);
    wal.open();

    WALReader reader = wal.getReader();
    WALWriter writer = wal.getWriter();

    Assert.assertNotNull(reader);
    Assert.assertNotNull(writer);

    WALEntry e1 = new MockWALEntry();

    writer.open();
    writer.write(e1);
    Assert.assertNull(reader.next());

    writer.mark();
    writer.close();

    WALEntry e2 = reader.next();

    Assert.assertEquals(e1, e2);
    Assert.assertNull(reader.next());

    wal.close();
  }

  public static class MockWAL implements WAL {

    private LinkedList<WALEntry> queue;

    public MockWAL() {
      queue = new LinkedList<WALEntry>();
    }

    @Override
    public void configure(Map<String, String> properties) {
      logger.debug("configure wal - properties:{}", properties);
    }

    @Override
    public void open() {
      logger.debug("open wal");
    }

    @Override
    public void close() {
      logger.debug("close wal");
    }

    @Override
    public WALReader getReader() {
      MockWALReader reader = new MockWALReader();

      reader.wal = this;

      return reader;
    }

    @Override
    public WALWriter getWriter() {
      MockWALWriter writer = new MockWALWriter();

      writer.wal = this;

      return writer;
    }

    public void write(Collection<WALEntry> entries) {
      queue.addAll(entries);
    }

    public WALEntry next() {
      if (!queue.isEmpty()) {
        return queue.pop();
      }

      return null;
    }

    public static class MockWALReader implements WALReader {

      private MockWAL wal;

      @Override
      public void open() {

      }

      @Override
      public WALEntry next() {
        return wal.next();
      }

      @Override
      public void close() {

      }

      @Override
      public void mark() {

      }

      @Override
      public void reset() {

      }

    }

    public static class MockWALWriter implements WALWriter {

      private MockWAL wal;
      private LinkedList<WALEntry> queue;

      public MockWALWriter() {
        queue = new LinkedList<WALEntry>();
      }

      @Override
      public void open() {
        logger.debug("opening writer");
      }

      @Override
      public void write(WALEntry entry) {
        logger.debug("writing entry:{}", entry);

        queue.add(entry);
      }

      @Override
      public void mark() {
        logger.debug("synchronizing writer - {} elements", queue.size());

        wal.write(queue);

        queue.clear();
      }

      @Override
      public void reset() {

      }

      @Override
      public void close() {
        logger.debug("closing writer");
      }

    }

    public static class MockWALEntry implements WALEntry {

    }

  }

}
