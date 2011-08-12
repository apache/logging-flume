package org.apache.flume.event;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;

public class EventBuilder {

  public static <T> Event<T> withBody(T body) {
    Event<T> event = new SimpleEvent<T>();

    event.setBody(body);

    return event;
  }

  public static <T> Event<T> withBody(T body, Map<String, String> headers) {
    Event<T> event = new SimpleEvent<T>();

    event.setBody(body);
    event.setHeaders(new HashMap<String, String>(headers));

    return event;
  }

}
