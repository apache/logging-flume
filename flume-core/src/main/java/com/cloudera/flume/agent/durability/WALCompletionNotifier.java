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

package com.cloudera.flume.agent.durability;

import java.io.IOException;

/**
 * Slice off just the interface we need from WALManager to reduce coupling with
 * this and the LivenessManager. WALManager now implements this.
 * 
 * TODO (jon) This a is a quick hack for now. Fold this in with AckListener to
 * reduce the number of classes.
 */
public interface WALCompletionNotifier {

  /**
   * Transition a dataset tagged with tag from SENT state to E2EACKED state.
   */
  public void toAcked(String tag) throws IOException;

  /**
   * Transition a data set tagged with SENT to LOGGED for it to be retried.
   */
  public void retry(String tag) throws IOException;

}
