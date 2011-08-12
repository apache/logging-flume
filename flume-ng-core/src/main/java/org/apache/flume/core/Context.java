package org.apache.flume.core;

import java.util.HashMap;
import java.util.Map;

public class Context {

  private Map<String, Object> parameters;

  public Context() {
    parameters = new HashMap<String, Object>();
  }

  public void put(String key, Object value) {
    parameters.put(key, value);
  }

  public <T> T get(String key, Class<? extends T> clazz) {
    if (parameters.containsKey(key)) {
      return clazz.cast(parameters.get(key));
    }

    return null;
  }

  @Override
  public String toString() {
    return "{ parameters:" + parameters + " }";
  }

  public Map<String, Object> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, Object> parameters) {
    this.parameters = parameters;
  }

}
