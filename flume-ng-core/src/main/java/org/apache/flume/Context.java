package org.apache.flume;

import java.util.HashMap;
import java.util.Map;

public class Context {

  private Map<String, Object> parameters;
  private Reporter reporter;

  public Context() {
    parameters = new HashMap<String, Object>();
    reporter = new Reporter();
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
    return "{ parameters:" + parameters + " reporter:" + reporter + " }";
  }

  public Map<String, Object> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, Object> parameters) {
    this.parameters = parameters;
  }

  public Reporter getReporter() {
    return reporter;
  }

  public void setReporter(Reporter reporter) {
    this.reporter = reporter;
  }

}
