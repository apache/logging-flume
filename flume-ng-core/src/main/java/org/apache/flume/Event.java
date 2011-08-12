package org.apache.flume;

import java.util.Map;

public interface Event<T> {

  public Map<String, String> getHeaders();

  public void setHeaders(Map<String, String> headers);

  public T getBody();

  public void setBody(T body);

}
