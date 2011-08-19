package org.apache.flume.sink;

import java.io.File;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRollingFileSink {

  private static final Logger logger = LoggerFactory
      .getLogger(TestRollingFileSink.class);

  private File tmpDir;
  private RollingFileSink sink;

  @Before
  public void setUp() {
    tmpDir = new File("/tmp/flume-rfs-" + System.currentTimeMillis() + "-"
        + Thread.currentThread().getId());

    sink = new RollingFileSink();

    tmpDir.mkdirs();

    sink.setDirectory(tmpDir);
  }

  @After
  public void tearDown() {
    tmpDir.delete();
  }

  @Test
  public void testLifecycle() throws InterruptedException, LifecycleException {
    Context context = new Context();

    sink.open(context);
    sink.close(context);
  }

  @Test
  public void testAppend() throws InterruptedException, LifecycleException,
      EventDeliveryException {

    Context context = new Context();

    sink.setRollInterval(1);

    sink.open(context);

    for (int i = 0; i < 10; i++) {
      Event event = new SimpleEvent();

      event.setBody(("Test event " + i).getBytes());
      sink.append(context, event);

      Thread.sleep(500);
    }

    sink.close(context);

    for (String file : sink.getDirectory().list()) {
      logger.debug("Produced file:{}", file);
    }
  }
}
