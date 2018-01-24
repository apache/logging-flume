package org.apache.flume.conf.sink;

/**
 * This is a mock to avoid the circular dependency in tests
 * TODO fix wrong dependency directions in the project config should not depend on an implementation
 */
public class NullSinkConfiguration extends SinkConfiguration {
  public NullSinkConfiguration(String componentName) {
    super(componentName);
  }
}
