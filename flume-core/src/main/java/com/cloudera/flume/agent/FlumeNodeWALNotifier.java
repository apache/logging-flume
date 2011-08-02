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

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.agent.durability.WALCompletionNotifier;
import com.cloudera.flume.agent.durability.WALManager;

/**
 * This is just here to make toAcked and retry for a map look like a single
 * operation so I don't have to change the liveness manager.
 * 
 * This relies on the fact that this is a reference to a map that is modified
 * elsewhere (e.g. in FlumeNode)
 */
public class FlumeNodeWALNotifier implements WALCompletionNotifier {
  public static final Logger LOG = LoggerFactory.getLogger(FlumeNodeWALNotifier.class);
  final Map<String, WALManager> node;

  /**
   * Pick an arbitrary node.
   */
  public FlumeNodeWALNotifier(Map<String, WALManager> node) {
    this.node = node;
  }

  /**
   * This takes a tag and attempts to retry each in each wal.
   * */
  @Override
  public void retry(String tag) throws IOException {
    Map<String, WALManager> mp = node;
    for (WALManager wm : mp.values()) {
      wm.retry(tag);
    }
  }

  /**
   * This takes a tag and attempts to move a chunk with that tag to the acked
   * state.
   * */
  @Override
  public void toAcked(String tag) throws IOException {
    Map<String, WALManager> mp = node;
    int success = 0;

    for (WALManager wm : mp.values()) {
      try {
        wm.toAcked(tag);
        success++;
      } catch (Exception ioe) {
        // We are being lax here -- we will fail on each logical node except for
        // the proper one. Thus this must catch IOExceptions and
        // IllegalState/IllegalArgument Exceptions

        // eat it.
        LOG.debug(ioe.getMessage(), ioe);
      }
    }

    if (success == 0) {
      // this is an odd situation
      LOG.warn("No wal managers contained tag " + tag);
    }

    if (success > 1) {
      // this is weird too
      LOG.warn("Expected exactly one wal manager to contain tag " + tag
          + " but " + success + "did!");
    }
  }
}
