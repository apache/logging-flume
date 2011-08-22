package org.apache.flume.formatter.output;

import org.apache.flume.Event;

public class TextDelimitedOutputFormatter implements EventFormatter {

  @Override
  public byte[] format(Event event) {
    String body = event.getBody().length > 0 ? new String(event.getBody()) : "";

    return (body + "\n").getBytes();
  }

}
