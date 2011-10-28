package org.apache.flume;

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

  public <T> T get(String key, Class<? extends T> clazz, T defaultValue) {
    T result = get(key, clazz);
    if (result == null) {
      result = defaultValue;
    }

    return result;
  }

  public String getString(String key) {
    return get(key, String.class);
  }

  public String getString(String key, String defaultValue) {
    return get(key, String.class, defaultValue);
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

  public void clear() {
    parameters.clear();
  }
}
