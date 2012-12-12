/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.agent.embedded;

import java.util.Map;

import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.node.MaterializedConfiguration;

/**
 * Provides {@see MaterializedConfiguration} for a given agent and set of
 * properties. This class exists simply to make more easily testable. That is
 * it allows us to mock the actual Source, Sink, and Channel components
 * as opposed to instantiation of real components.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class MaterializedConfigurationProvider {

  MaterializedConfiguration get(String name, Map<String, String> properties) {
    MemoryConfigurationProvider confProvider =
        new MemoryConfigurationProvider(name, properties);
    return confProvider.getConfiguration();
  }
}
