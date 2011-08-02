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
package com.cloudera.flume.handlers.endtoend;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.cloudera.flume.agent.MasterRPC;

/**
 * This is for a collector to send notify the master of acknowledgements from
 * successfully received tagged groups.
 */
public class CollectorAckListener extends AckListener.Empty {
  final static Logger LOG = Logger.getLogger(CollectorAckListener.class
      .getName());

  MasterRPC c;

  public CollectorAckListener(MasterRPC c) {
    this.c = c;
  }

  @Override
  public void end(String group) throws IOException {
    try {
      c.acknowledge(group);
      LOG.debug("acknowledge sent to master: " + group);
    } catch (IOException te) {
      LOG.error("acknowledge failed", te);
      throw new IOException(te);
    }
  }
}
