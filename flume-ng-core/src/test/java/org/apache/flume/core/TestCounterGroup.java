package org.apache.flume.core;

import java.util.concurrent.atomic.AtomicLong;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class TestCounterGroup {

  private CounterGroup counterGroup;

  @Before
  public void setUp() {
    counterGroup = new CounterGroup();
  }

  @Test
  public void testGetCounter() {
    AtomicLong counter = counterGroup.getCounter("test");

    Assert.assertNotNull(counter);
    Assert.assertEquals(0, counter.get());
  }

  @Test
  public void testGet() {
    long value = counterGroup.get("test");

    Assert.assertEquals(0, value);
  }

  @Test
  public void testIncrementAndGet() {
    long value = counterGroup.incrementAndGet("test");

    Assert.assertEquals(1, value);
  }

  @Test
  public void testAddAndGet() {
    long value = counterGroup.addAndGet("test", 13L);

    Assert.assertEquals(13, value);
  }

}
