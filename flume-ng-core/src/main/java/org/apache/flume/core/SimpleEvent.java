package org.apache.flume.core;

import java.util.HashMap;
import java.util.Map;

public class SimpleEvent<T> implements Event<T> {

  private Map<String, String> headers;
  private T body;

  public SimpleEvent() {
    headers = new HashMap<String, String>();
    body = null;
  }

  @Override
  public Map<String, String> getHeaders() {
    return headers;
  }

  @Override
  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }

  @Override
  public T getBody() {
    return body;
  }

  @Override
  public void setBody(T body) {
    this.body = body;
  }

  @Override
  public String toString() {
    return "{ headers:" + headers + " body:" + body + " }";
  }

}
