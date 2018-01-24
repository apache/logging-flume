package org.apache.flume.conf.channel;

/**
 * This is a mock to avoid the circular dependency in tests
 * TODO fix wrong dependency directions in the project config should not depend on an implementation
 */
public class MemoryChannelConfiguration extends ChannelConfiguration {
  public MemoryChannelConfiguration(String componentName) {
    super(componentName);
  }
}
