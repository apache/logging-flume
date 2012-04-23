/**
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

public enum FlumeConfigurationErrorType {
  OTHER(null),
  AGENT_CONFIGURATION_INVALID("Agent configuration is invalid."),
  PROPERTY_NAME_NULL("Property needs a name."),
  PROPERTY_VALUE_NULL("Property value missing."),
  AGENT_NAME_MISSING("Agent name is required."),
  CONFIGURATION_KEY_ERROR("Configuration Key is invalid."),
  DUPLICATE_PROPERTY("Property already configured."),
  INVALID_PROPERTY("No such property."),
  PROPERTY_PART_OF_ANOTHER_GROUP("This property is part of another group."),
  ATTRS_MISSING("Required attributes missing."),
  ILLEGAL_PROPERTY_NAME("This attribute name is invalid."),
  DEFAULT_VALUE_ASSIGNED(
      "Value in configuration is invalid for this key, assigned default value."),
  CONFIG_ERROR("Configuration of component failed.");
  private final String error;

  private FlumeConfigurationErrorType(String error) {
    this.error = error;
  }

  public String getError() {
    return error;
  }
}
