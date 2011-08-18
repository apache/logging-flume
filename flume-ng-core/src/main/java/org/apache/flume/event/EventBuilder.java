package org.apache.flume.event;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;

public class EventBuilder {

  public static Event withBody(byte[] body) {
    Event event = new SimpleEvent();

    event.setBody(body);

    return event;
  }

  public static Event withBody(byte[] body, Map<String, String> headers) {
    Event event = new SimpleEvent();

    event.setBody(body);
    event.setHeaders(new HashMap<String, String>(headers));

    return event;
  }

}
