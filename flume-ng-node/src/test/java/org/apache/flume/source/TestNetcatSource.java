package org.apache.flume.source;

import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestNetcatSource {

  private Channel channel;
  private EventDrivenSource source;

  @Before
  public void setUp() {
    channel = new MemoryChannel();
    source = new NetcatSource();

    Context context = new Context();
    context.put("capacity", 50);

    Configurables.configure(channel, context);

    source.setChannel(channel);
  }

  @Test
  public void testLifecycle() throws InterruptedException, LifecycleException,
      EventDeliveryException {

    ExecutorService executor = Executors.newFixedThreadPool(3);
    Context context = new Context();

    /* FIXME: Use a random port for testing. */
    context.put("name", "test");
    context.put("port", 41414);

    Configurables.configure(source, context);

    source.start();

    /* FIXME: Ensure proper send / received semantics. */

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

    Transaction tx = source.getChannel().getTransaction();
    tx.begin();

    for (int i = 0; i < 100; i++) {
      executor.submit(clientRequestRunnable);

      Event event = channel.take();

      Assert.assertNotNull(event);
      Assert.assertArrayEquals("Test message".getBytes(), event.getBody());
    }

    tx.commit();
    tx.close();
    executor.shutdown();

    while (!executor.isTerminated()) {
      executor.awaitTermination(500, TimeUnit.MILLISECONDS);
    }

    source.stop();
  }

}
