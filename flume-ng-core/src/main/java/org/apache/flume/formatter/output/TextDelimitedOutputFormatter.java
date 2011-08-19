package org.apache.flume.formatter.output;

import org.apache.flume.Event;

public class TextDelimitedOutputFormatter {

  public byte[] format(Event event) {
    return (new String(event.getBody()) + "\n").getBytes();
  }

}
