package org.apache.flume.source;

import java.io.IOException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.durability.WAL;
import org.apache.flume.durability.WALManager;
import org.apache.flume.durability.WALWriter;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.lifecycle.LifecycleException;

public class SequenceGeneratorSource extends AbstractEventSource implements
    Configurable {

  private String nodeName;

  private long sequence;
  private WALManager walManager;
  private WALWriter walWriter;

  @Override
  public void open(Context context) throws LifecycleException {
    if (walManager != null) {
      WAL wal = walManager.getWAL(nodeName);
      try {
        walWriter = wal.getWriter();
      } catch (IOException e) {
        throw new LifecycleException(e);
      }
    }
  }

  @Override
  public void close(Context context) throws LifecycleException {
    if (walWriter != null) {
      try {
        walWriter.close();
      } catch (IOException e) {
        throw new LifecycleException(e);
      }
    }
  }

  @Override
  public Event next(Context context) throws InterruptedException,
      EventDeliveryException {

    Event event = new SimpleEvent();

    event.setBody(Long.valueOf(sequence++).toString().getBytes());

    if (walWriter != null) {
      try {
        walWriter.write(event);
        walWriter.flush();
      } catch (IOException e) {
        throw new EventDeliveryException("Unable to write event to WAL via "
            + walWriter + ". Exception follows.", e);
      }
    }

    return event;
  }

  @Override
  public void configure(Context context) {
    Configurables.ensureRequiredNonNull(context, "logicalNode.name");

    nodeName = context.get("logicalNode.name", String.class);
  }

  public WALManager getWALManager() {
    return walManager;
  }

  public void setWALManager(WALManager walManager) {
    this.walManager = walManager;
  }

}
