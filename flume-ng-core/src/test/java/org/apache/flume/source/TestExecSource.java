package org.apache.flume.source;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestExecSource {

  private AbstractSource source;

  @Before
  public void setUp() {
    source = new ExecSource();
  }

  @Test
  public void testProcess() throws InterruptedException, LifecycleException,
      EventDeliveryException, IOException {

    Channel channel = new MemoryChannel();
    Context context = new Context();

    context.put("command", "cat /etc/passwd");
    Configurables.configure(source, context);
    Configurables.configure(channel, context);

    source.setChannel(channel);
    source.start();
    Transaction transaction = channel.getTransaction();

    transaction.begin();
    Event event;
    int numEvents = 0;

    FileOutputStream outputStream = new FileOutputStream(
        "/tmp/flume-execsource." + Thread.currentThread().getId());

    while ((event = channel.take()) != null) {
      outputStream.write(event.getBody());
      outputStream.write('\n');
      numEvents++;
    }

    outputStream.close();
    transaction.commit();
    transaction.close();

    source.stop();

    File file1 = new File("/tmp/flume-execsource."
        + Thread.currentThread().getId());
    File file2 = new File("/etc/passwd");
    Assert.assertEquals(FileUtils.checksumCRC32(file1),
        FileUtils.checksumCRC32(file2));
    FileUtils.forceDelete(file1);
  }

}
