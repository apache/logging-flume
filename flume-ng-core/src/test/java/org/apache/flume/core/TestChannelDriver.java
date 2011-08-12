package org.apache.flume.core;

import junit.framework.Assert;

import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.sink.NullSink;
import org.apache.flume.source.SequenceGeneratorSource;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestChannelDriver {

  private static final Logger logger = LoggerFactory
      .getLogger(TestChannelDriver.class);

  private ChannelDriver driver;

  @Before
  public void setUp() {
    driver = new ChannelDriver("test-channel-driver");
  }

  @Test
  public void testNormalLifecycle() throws LifecycleException,
      InterruptedException {
    final EventSource source = new SequenceGeneratorSource();
    final EventSink sink = new NullSink();
    final CounterGroup sourceCounters = new CounterGroup();
    final CounterGroup sinkCounters = new CounterGroup();

    sourceCounters.setName("source");
    sinkCounters.setName("sink");

    driver.setSource(new EventSource() {

      @Override
      public Event<?> next(Context context) throws InterruptedException,
          MessageDeliveryException {

        sourceCounters.incrementAndGet("next");

        return source.next(context);
      }

      @Override
      public void open(Context context) throws InterruptedException,
          LifecycleException {

        sourceCounters.incrementAndGet("open");

        source.open(context);
      }

      @Override
      public void close(Context context) throws InterruptedException,
          LifecycleException {

        sourceCounters.incrementAndGet("close");

        source.open(context);
      }
    });

    driver.setSink(new EventSink() {

      @Override
      public void append(Context context, Event<?> event)
          throws InterruptedException, MessageDeliveryException {

        sinkCounters.incrementAndGet("append");

        sink.append(context, event);
      }

      @Override
      public void open(Context context) throws InterruptedException,
          LifecycleException {

        sinkCounters.incrementAndGet("open");

        sink.open(context);
      }

      @Override
      public void close(Context context) throws InterruptedException,
          LifecycleException {

        sinkCounters.incrementAndGet("close");

        sink.close(context);
      }
    });

    Context context = new Context();

    driver.start(context);

    LifecycleController.waitForOneOf(driver, new LifecycleState[] {
        LifecycleState.START, LifecycleState.ERROR }, 5000);

    Assert.assertEquals(LifecycleState.START, driver.getLifecycleState());

    Thread.sleep(500);

    driver.stop(context);

    LifecycleController.waitForOneOf(driver, new LifecycleState[] {
        LifecycleState.STOP, LifecycleState.ERROR }, 5000);

    Assert.assertEquals(LifecycleState.STOP, driver.getLifecycleState());

    Assert.assertEquals(Long.valueOf(1), sourceCounters.get("open"));
    Assert.assertEquals(Long.valueOf(1), sourceCounters.get("close"));
    Assert.assertTrue(sourceCounters.get("next") > 0);
    Assert.assertEquals(Long.valueOf(1), sinkCounters.get("open"));
    Assert.assertEquals(Long.valueOf(1), sinkCounters.get("close"));
    Assert.assertTrue(sinkCounters.get("append") > 0);
    Assert.assertEquals(
        "Source next() events didn't match sink append() events",
        sourceCounters.get("next"), sinkCounters.get("append"));

    logger.info("Source counters:{} Sink counters:{}", sourceCounters,
        sinkCounters);
  }

  @Test
  public void testFailedOpen() throws LifecycleException, InterruptedException {
    final EventSource source = new SequenceGeneratorSource();
    final EventSink sink = new NullSink();
    final CounterGroup sourceCounters = new CounterGroup();
    final CounterGroup sinkCounters = new CounterGroup();

    sourceCounters.setName("source");
    sinkCounters.setName("sink");

    driver.setSource(new EventSource() {

      @Override
      public Event<?> next(Context context) throws InterruptedException,
          MessageDeliveryException {

        sourceCounters.incrementAndGet("next");

        return source.next(context);
      }

      @Override
      public void open(Context context) throws InterruptedException,
          LifecycleException {

        throw new LifecycleException("Open failed!");
      }

      @Override
      public void close(Context context) throws InterruptedException,
          LifecycleException {

        sourceCounters.incrementAndGet("close");

        source.open(context);
      }
    });

    driver.setSink(new EventSink() {

      @Override
      public void append(Context context, Event<?> event)
          throws InterruptedException, MessageDeliveryException {

        sinkCounters.incrementAndGet("append");

        sink.append(context, event);
      }

      @Override
      public void open(Context context) throws InterruptedException,
          LifecycleException {

        throw new LifecycleException("Open failed!");
      }

      @Override
      public void close(Context context) throws InterruptedException,
          LifecycleException {

        sinkCounters.incrementAndGet("close");

        sink.close(context);
      }
    });

    Context context = new Context();

    driver.start(context);

    LifecycleController.waitForOneOf(driver, new LifecycleState[] {
        LifecycleState.START, LifecycleState.ERROR }, 5000);

    Assert.assertEquals(LifecycleState.START, driver.getLifecycleState());

    Thread.sleep(500);

    driver.stop(context);

    LifecycleController.waitForOneOf(driver, new LifecycleState[] {
        LifecycleState.STOP, LifecycleState.ERROR }, 5000);

    Assert.assertEquals(LifecycleState.ERROR, driver.getLifecycleState());

    Assert.assertEquals(Long.valueOf(0), sourceCounters.get("open"));
    Assert.assertEquals(Long.valueOf(0), sourceCounters.get("close"));
    Assert.assertEquals(Long.valueOf(0), sourceCounters.get("next"));
    Assert.assertEquals(Long.valueOf(0), sinkCounters.get("open"));
    Assert.assertEquals(Long.valueOf(0), sinkCounters.get("close"));
    Assert.assertEquals(Long.valueOf(0), sinkCounters.get("append"));
    Assert.assertEquals(
        "Source next() events do not match sink append() events",
        sourceCounters.get("next"), sinkCounters.get("append"));

    logger.info("Source counters:{} Sink counters:{}", sourceCounters,
        sinkCounters);
  }

  @Test
  public void testFailedClose() throws LifecycleException, InterruptedException {
    final EventSource source = new SequenceGeneratorSource();
    final EventSink sink = new NullSink();
    final CounterGroup sourceCounters = new CounterGroup();
    final CounterGroup sinkCounters = new CounterGroup();

    sourceCounters.setName("source");
    sinkCounters.setName("sink");

    driver.setSource(new EventSource() {

      @Override
      public Event<?> next(Context context) throws InterruptedException,
          MessageDeliveryException {

        sourceCounters.incrementAndGet("next");

        return source.next(context);
      }

      @Override
      public void open(Context context) throws InterruptedException,
          LifecycleException {

        sourceCounters.incrementAndGet("open");

        source.open(context);
      }

      @Override
      public void close(Context context) throws InterruptedException,
          LifecycleException {

        throw new LifecycleException("Close failed because I said so!");
      }
    });

    driver.setSink(new EventSink() {

      @Override
      public void append(Context context, Event<?> event)
          throws InterruptedException, MessageDeliveryException {

        sinkCounters.incrementAndGet("append");

        sink.append(context, event);
      }

      @Override
      public void open(Context context) throws InterruptedException,
          LifecycleException {

        sinkCounters.incrementAndGet("open");

        sink.open(context);
      }

      @Override
      public void close(Context context) throws InterruptedException,
          LifecycleException {

        throw new LifecycleException("Close failed because I said so!");
      }
    });

    Context context = new Context();

    driver.start(context);

    LifecycleController.waitForOneOf(driver, new LifecycleState[] {
        LifecycleState.START, LifecycleState.ERROR }, 5000);

    Assert.assertEquals(LifecycleState.START, driver.getLifecycleState());

    Thread.sleep(500);

    driver.stop(context);

    LifecycleController.waitForOneOf(driver, new LifecycleState[] {
        LifecycleState.STOP, LifecycleState.ERROR }, 5000);

    Assert.assertEquals(LifecycleState.ERROR, driver.getLifecycleState());

    Assert.assertEquals(Long.valueOf(1), sourceCounters.get("open"));
    Assert.assertEquals(Long.valueOf(0), sourceCounters.get("close"));
    Assert.assertTrue(sourceCounters.get("next") > 0);
    Assert.assertEquals(Long.valueOf(1), sinkCounters.get("open"));
    Assert.assertEquals(Long.valueOf(0), sinkCounters.get("close"));
    Assert.assertTrue(sinkCounters.get("append") > 0);
    Assert.assertEquals(
        "Source next() events do not match sink append() events",
        sourceCounters.get("next"), sinkCounters.get("append"));

    logger.info("Source counters:{} Sink counters:{}", sourceCounters,
        sinkCounters);
  }

  @Test
  public void testFlakeyNext() throws LifecycleException, InterruptedException {
    final EventSource source = new SequenceGeneratorSource();
    final EventSink sink = new NullSink();
    final CounterGroup sourceCounters = new CounterGroup();
    final CounterGroup sinkCounters = new CounterGroup();

    sourceCounters.setName("source");
    sinkCounters.setName("sink");

    driver.setSource(new EventSource() {

      @Override
      public Event<?> next(Context context) throws InterruptedException,
          MessageDeliveryException {

        if (Math.round(Math.random()) == 0) {
          throw new MessageDeliveryException("I don't feel like working.");
        }

        sourceCounters.incrementAndGet("next");

        return source.next(context);
      }

      @Override
      public void open(Context context) throws InterruptedException,
          LifecycleException {

        sourceCounters.incrementAndGet("open");

        source.open(context);
      }

      @Override
      public void close(Context context) throws InterruptedException,
          LifecycleException {

        sourceCounters.incrementAndGet("close");

        source.close(context);
      }
    });

    driver.setSink(new EventSink() {

      @Override
      public void append(Context context, Event<?> event)
          throws InterruptedException, MessageDeliveryException {

        sinkCounters.incrementAndGet("append");

        sink.append(context, event);
      }

      @Override
      public void open(Context context) throws InterruptedException,
          LifecycleException {

        sinkCounters.incrementAndGet("open");

        sink.open(context);
      }

      @Override
      public void close(Context context) throws InterruptedException,
          LifecycleException {

        sinkCounters.incrementAndGet("close");

        sink.close(context);
      }
    });

    Context context = new Context();

    driver.start(context);

    LifecycleController.waitForOneOf(driver, new LifecycleState[] {
        LifecycleState.START, LifecycleState.ERROR }, 5000);

    Assert.assertEquals(LifecycleState.START, driver.getLifecycleState());

    Thread.sleep(500);

    driver.stop(context);

    LifecycleController.waitForOneOf(driver, new LifecycleState[] {
        LifecycleState.STOP, LifecycleState.ERROR }, 5000);

    Assert.assertEquals(LifecycleState.STOP, driver.getLifecycleState());

    Assert.assertEquals(Long.valueOf(1), sourceCounters.get("open"));
    Assert.assertEquals(Long.valueOf(1), sourceCounters.get("close"));
    Assert.assertTrue(sourceCounters.get("next") > 0);
    Assert.assertEquals(Long.valueOf(1), sinkCounters.get("open"));
    Assert.assertEquals(Long.valueOf(1), sinkCounters.get("close"));
    Assert.assertTrue(sinkCounters.get("append") > 0);
    Assert.assertEquals(
        "Source next() events do not match sink append() events",
        sourceCounters.get("next"), sinkCounters.get("append"));

    logger.info("Source counters:{} Sink counters:{}", sourceCounters,
        sinkCounters);
  }

  @Test
  public void testFlakeyAppend() throws LifecycleException,
      InterruptedException {

    final EventSource source = new SequenceGeneratorSource();
    final EventSink sink = new NullSink();
    final CounterGroup sourceCounters = new CounterGroup();
    final CounterGroup sinkCounters = new CounterGroup();

    sourceCounters.setName("source");
    sinkCounters.setName("sink");

    driver.setSource(new EventSource() {

      @Override
      public Event<?> next(Context context) throws InterruptedException,
          MessageDeliveryException {

        sourceCounters.incrementAndGet("next");

        return source.next(context);
      }

      @Override
      public void open(Context context) throws InterruptedException,
          LifecycleException {

        sourceCounters.incrementAndGet("open");

        source.open(context);
      }

      @Override
      public void close(Context context) throws InterruptedException,
          LifecycleException {

        sourceCounters.incrementAndGet("close");

        source.close(context);
      }
    });

    driver.setSink(new EventSink() {

      @Override
      public void append(Context context, Event<?> event)
          throws InterruptedException, MessageDeliveryException {

        if (Math.round(Math.random()) == 0) {
          throw new MessageDeliveryException("I don't feel like working.");
        }

        sinkCounters.incrementAndGet("append");

        sink.append(context, event);
      }

      @Override
      public void open(Context context) throws InterruptedException,
          LifecycleException {

        sinkCounters.incrementAndGet("open");

        sink.open(context);
      }

      @Override
      public void close(Context context) throws InterruptedException,
          LifecycleException {

        sinkCounters.incrementAndGet("close");

        sink.close(context);
      }
    });

    Context context = new Context();

    driver.start(context);

    LifecycleController.waitForOneOf(driver, new LifecycleState[] {
        LifecycleState.START, LifecycleState.ERROR }, 5000);

    Assert.assertEquals(LifecycleState.START, driver.getLifecycleState());

    Thread.sleep(500);

    driver.stop(context);

    LifecycleController.waitForOneOf(driver, new LifecycleState[] {
        LifecycleState.STOP, LifecycleState.ERROR }, 5000);

    Assert.assertEquals(LifecycleState.STOP, driver.getLifecycleState());

    Assert.assertEquals(Long.valueOf(1), sourceCounters.get("open"));
    Assert.assertEquals(Long.valueOf(1), sourceCounters.get("close"));
    Assert.assertTrue(sourceCounters.get("next") > 0);
    Assert.assertEquals(Long.valueOf(1), sinkCounters.get("open"));
    Assert.assertEquals(Long.valueOf(1), sinkCounters.get("close"));
    Assert.assertTrue(sinkCounters.get("append") > 0);

    logger.info("Source counters:{} Sink counters:{}", sourceCounters,
        sinkCounters);
  }

}
