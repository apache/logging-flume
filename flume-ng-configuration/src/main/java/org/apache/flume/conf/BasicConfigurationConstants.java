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

public final class BasicConfigurationConstants {

  public static final String CONFIG_SOURCES = "sources";
  public static final String CONFIG_SOURCES_PREFIX = CONFIG_SOURCES + ".";
  public static final String CONFIG_SOURCE_CHANNELSELECTOR_PREFIX = "selector.";

  public static final String CONFIG_SINKS = "sinks";
  public static final String CONFIG_SINKS_PREFIX = CONFIG_SINKS + ".";
  public static final String CONFIG_SINK_PROCESSOR_PREFIX = "processor.";

  public static final String CONFIG_SINKGROUPS = "sinkgroups";
  public static final String CONFIG_SINKGROUPS_PREFIX = CONFIG_SINKGROUPS + ".";

  public static final String CONFIG_CHANNEL = "channel";
  public static final String CONFIG_CHANNELS = "channels";
  public static final String CONFIG_CHANNELS_PREFIX = CONFIG_CHANNELS + ".";

  public static final String CONFIG_CONFIG = "config";
  public static final String CONFIG_TYPE = "type";

  private BasicConfigurationConstants() {
    // disable explicit object creation
  }

}
