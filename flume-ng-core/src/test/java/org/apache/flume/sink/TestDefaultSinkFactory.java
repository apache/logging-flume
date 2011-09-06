package org.apache.flume.sink;

import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDefaultSinkFactory {

  private SinkFactory sinkFactory;

  @Before
  public void setUp() {
    sinkFactory = new DefaultSinkFactory();
  }

  @Test
  public void testRegister() {
    Assert.assertEquals(0, sinkFactory.getSinkNames().size());

    sinkFactory.register("null", NullSink.class);

    Assert.assertEquals(1, sinkFactory.getSinkNames().size());

    Assert.assertEquals("null", sinkFactory.getSinkNames().iterator().next());
  }

  @Test
  public void testCreate() throws InstantiationException {
    Assert.assertEquals(0, sinkFactory.getSinkNames().size());

    sinkFactory.register("null", NullSink.class);

    Assert.assertEquals(1, sinkFactory.getSinkNames().size());

    Assert.assertEquals("null", sinkFactory.getSinkNames().iterator().next());

    Sink sink = sinkFactory.create("null");

    Assert.assertNotNull("Factory returned a null sink", sink);
    Assert.assertTrue("Source isn't an instance of NullSink",
        sink instanceof NullSink);

    sink = sinkFactory.create("i do not exist");

    Assert.assertNull("Factory returned a sink it shouldn't have", sink);
  }

  @Test
  public void testUnregister() {
    Assert.assertEquals(0, sinkFactory.getSinkNames().size());

    Assert.assertTrue("Registering a source returned false",
        sinkFactory.register("null", NullSink.class));

    Assert.assertEquals(1, sinkFactory.getSinkNames().size());

    Assert.assertEquals("null", sinkFactory.getSinkNames().iterator().next());

    Assert.assertFalse("Unregistering an unknown sink returned true",
        sinkFactory.unregister("i do not exist"));
    Assert.assertTrue("Unregistering a sink returned false",
        sinkFactory.unregister("null"));

    Assert.assertEquals(0, sinkFactory.getSinkNames().size());
  }

}
