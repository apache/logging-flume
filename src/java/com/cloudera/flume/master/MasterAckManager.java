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
package com.cloudera.flume.master;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

/**
 * This is a master side ack tag manager.
 */
public class MasterAckManager {
  final static Logger LOG = Logger.getLogger(MasterAckManager.class.getName());

  Set<String> acked = new HashSet<String>();

  synchronized public void acknowledge(String ackid) {
    acked.add(ackid);
  }

  synchronized public boolean check(String ackid) {

    boolean committed = acked.contains(ackid);
    if (committed) {
      // For now, just keep all of them

      acked.remove(ackid);

      // TODO (jon) age off

      // LOG.debug("Master removed " + ackid + " from tracked acks");

      // this is destructive, but minimizes state.
      // There is a failure case here that potentially causes duplicates.
    }
    return committed;

  }

  /**
   * This method returns a copy of the current outstanding and completed ackIds
   * This method is currently only used in tests.
   */
  synchronized public Set<String> getPending() {
    return new HashSet<String>(acked);
  }

  public void dumpLog() {
    LOG.info("dumping ack manager state");

    for (String s : acked) {
      LOG.info(" " + s);
    }
  }

  public void start() throws IOException {
    // Does nothing
  }

  public void stop() {
    // Does nothing
  }
}
