package org.apache.flume.source;

import org.apache.flume.core.Context;
import org.apache.flume.core.Event;
import org.apache.flume.core.EventSource;
import org.apache.flume.core.EventDeliveryException;
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

    source.open(context);

    for (long i = 0; i < 100; i++) {
      Event<?> next = source.next(context);
      long value = (Long) next.getBody();

      Assert.assertEquals(i, value);
    }

    source.close(context);
  }

}
