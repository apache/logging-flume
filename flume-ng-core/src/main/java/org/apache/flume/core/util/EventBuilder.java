package org.apache.flume.core.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.core.Event;
import org.apache.flume.core.SimpleEvent;

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
