package org.apache.flume.formatter.output;

import org.apache.flume.Event;

public interface EventFormatter {

  public byte[] format(Event event);

}
