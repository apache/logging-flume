package org.apache.flume.conf.source.jms;

import org.apache.flume.conf.source.SourceConfiguration;

/**
 * This is a mock to avoid the circular dependency in tests
 * TODO fix wrong dependency directions in the project config should not depend on an implementation
 */
public class JMSSourceConfiguration extends SourceConfiguration {
  public JMSSourceConfiguration(String componentName) {
    super(componentName);
  }
}
