package org.apache.flume;

import java.util.Map;

public interface Event {

  public Map<String, String> getHeaders();

  public void setHeaders(Map<String, String> headers);

  public byte[] getBody();

  public void setBody(byte[] body);

}
