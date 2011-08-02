/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.flume.agent;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.faults.MemHog;

/**
 * This is a flume agent will eventually die due to an out of memory exception
 * caused by the MemHog.
 */
public class FaultyFlumeLocalAgentOOME extends FlumeNode {

  FaultyFlumeLocalAgentOOME(FlumeConfiguration conf) {
    super(conf);
  }

  public static void main(String[] argv) {
    FlumeConfiguration conf = FlumeConfiguration.get();
    FaultyFlumeLocalAgentOOME agent = new FaultyFlumeLocalAgentOOME(conf);
    agent.start();

    new MemHog().start();
    // hangout, waiting for other agent thread to exit.
  }
}
