package org.apache.flume.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestContext {

  private Context context;

  @Before
  public void setUp() {
    context = new Context();
  }

  @Test
  public void testPutGet() {
    Assert.assertEquals("Context is empty", 0, context.getParameters()
        .size());

    context.put("test", "test");

    Assert.assertEquals("Context contains test value", "test",
        context.get("test", String.class));
  }

}
