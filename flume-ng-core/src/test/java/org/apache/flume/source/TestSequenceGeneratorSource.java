package org.apache.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.EventSource;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestSequenceGeneratorSource {

  private EventSource source;

  @Before
  public void setUp() {
    source = new SequenceGeneratorSource();
  }

  @Test
  public void testNext() throws InterruptedException, LifecycleException,
      EventDeliveryException {

    Context context = new Context();

    context.put("logicalNode.name", "test");

    Configurables.configure(source, context);

    source.open(context);

    for (long i = 0; i < 100; i++) {
      Event next = source.next(context);
      long value = Long.parseLong(new String(next.getBody()));

      Assert.assertEquals(i, value);
    }

    source.close(context);
  }

}
