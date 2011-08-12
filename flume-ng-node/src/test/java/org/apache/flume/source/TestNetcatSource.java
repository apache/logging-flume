package org.apache.flume.source;

import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.EventSource;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNetcatSource {

  private EventSource source;

  @Before
  public void setUp() {
    source = new NetcatSource();
  }

  @Test(timeout = 5000)
  public void testLifecycle() throws InterruptedException, LifecycleException,
      EventDeliveryException {

    ExecutorService executor = Executors.newFixedThreadPool(3);
    Context context = new Context();

    /* FIXME: Use a random port for testing. */
    ((NetcatSource) source).setPort(41414);

    source.open(context);

    Runnable clientRequestRunnable = new Runnable() {

      @Override
      public void run() {
        try {
          SocketChannel clientChannel = SocketChannel
              .open(new InetSocketAddress(41414));

          Writer writer = Channels.newWriter(clientChannel, "utf-8");

          writer.write("Test message");

          writer.flush();
          clientChannel.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

    };

    for (int i = 0; i < 100; i++) {
      executor.submit(clientRequestRunnable);

      Event<?> event = source.next(context);

      Assert.assertNotNull(event);
      Assert.assertEquals("Test message", event.getBody());
    }

    executor.shutdown();

    while (!executor.isTerminated()) {
      executor.awaitTermination(500, TimeUnit.MILLISECONDS);
    }

    source.close(context);
  }

}
