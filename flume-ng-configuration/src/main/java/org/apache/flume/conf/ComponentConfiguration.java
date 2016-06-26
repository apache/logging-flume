/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.flume.conf;

import java.util.LinkedList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.conf.FlumeConfigurationError.ErrorOrWarning;

/**
 *
 * Abstract implementation of the Component Configuration Manager. This class
 * does the configuration in the object. The component specific versions, which
 * inherit from this, create the configuration based on the config file. All
 * subclasses of this class store properties of the component. The properties
 * can be stored as properties in this
 *
 */

public abstract class ComponentConfiguration {

  protected String componentName;

  private String type;
  protected boolean configured;
  protected List<FlumeConfigurationError> errors;
  private boolean notFoundConfigClass;

  public boolean isNotFoundConfigClass() {
    return notFoundConfigClass;
  }

  public void setNotFoundConfigClass() {
    this.notFoundConfigClass = true;
  }

  protected ComponentConfiguration(String componentName) {
    this.componentName = componentName;
    errors = new LinkedList<FlumeConfigurationError>();
    this.type = null;
    configured = false;
  }

  public List<FlumeConfigurationError> getErrors() {
    return errors;
  }

  public void configure(Context context) throws ConfigurationException {
    failIfConfigured();
    String confType = context.getString(
        BasicConfigurationConstants.CONFIG_TYPE);
    if (confType != null && !confType.isEmpty()) {
      this.type = confType;
    }
    // Type can be set by child class constructors, so check if it was.
    if (this.type == null || this.type.isEmpty()) {
      errors.add(new FlumeConfigurationError(componentName,
          BasicConfigurationConstants.CONFIG_TYPE,
          FlumeConfigurationErrorType.ATTRS_MISSING, ErrorOrWarning.ERROR));

      throw new ConfigurationException(
          "Component has no type. Cannot configure. " + componentName);
    }
  }

  protected void failIfConfigured() throws ConfigurationException {
    if (configured) {
      throw new ConfigurationException("Already configured component."
          + componentName);
    }
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String toString() {
    return toString(0);
  }

  public String toString(int indentCount) {
    StringBuilder indentSb = new StringBuilder("");

    for (int i = 0; i < indentCount; i++) {
      indentSb.append(FlumeConfiguration.INDENTSTEP);
    }

    String indent = indentSb.toString();
    StringBuilder sb = new StringBuilder(indent);

    sb.append("ComponentConfiguration[").append(componentName).append("]");
    sb.append(FlumeConfiguration.NEWLINE).append(indent).append(
        FlumeConfiguration.INDENTSTEP).append("CONFIG: ");
    sb.append(FlumeConfiguration.NEWLINE).append(indent).append(
        FlumeConfiguration.INDENTSTEP);

    return sb.toString();
  }

  public String getComponentName() {
    return componentName;
  }

  protected void setConfigured() {
    configured = true;
  }

  public enum ComponentType {
    OTHER(null),
    SOURCE("Source"),
    SINK("Sink"),
    SINK_PROCESSOR("SinkProcessor"),
    SINKGROUP("Sinkgroup"),
    CHANNEL("Channel"),
    CHANNELSELECTOR("ChannelSelector");

    private final String componentType;

    private ComponentType(String type) {
      componentType = type;
    }

    public String getComponentType() {
      return componentType;
    }
  }
}
