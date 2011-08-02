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
package com.cloudera.flume.agent.diskfailover;

import java.io.IOException;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.handlers.rolling.RollTrigger;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.flume.reporter.Reportable;

/**
 * This is the interface for providing durability of events until the reach the
 * permanent store. This is intended to be used after a detected failure and has
 * a slightly different state machine than the WALManager's. Different
 * implementations can be encapsulated by this interface.
 * 
 * Implementations of this interface must be thread safe -- it may be called
 * from different threads.
 */

public interface DiskFailoverManager extends Reportable {
  /**
   * These are the states a batch of events can be in.
   * 
   * WRITING means that there is some entity writing to the set of data.
   * 
   * LOGGED means that the data is durable on the node.
   * 
   * SENDING means that data is durable and is being sent somewhere downstream
   * 
   * SENT means that data has been received by the next downstream node. This
   * data can be deleted.
   * 
   * ERROR means that the set of data contains an error.
   */
  enum State {
    IMPORT, WRITING, LOGGED, SENDING, SENT, ERROR
  };

  final public static String A_MSG_WRITING = "writingEvts";
  final public static String A_MSG_READ = "readEvts";

  final public static String A_IMPORTED = "importedChunks";
  final public static String A_WRITING = "writingChunks";
  final public static String A_LOGGED = "loggedChunks";
  final public static String A_SENDING = "sendingChunks";
  final public static String A_SENT = "sentChunks";
  final public static String A_ACKED = "ackedChunks";
  final public static String A_ERROR = "errorChunks";
  final public static String A_RECOVERED = "recoveredChunks";
  final public static String A_IN_LOGGED = "loggedQChunks";

  /**
   * Open the WAL manager to enable reads and writes.
   */
  public void open() throws IOException;

  /**
   * Closes the WAL manager for reads and writes
   */
  public void close() throws IOException;

  /**
   * Recover data that is not in ERROR state assuming that data transmission has
   * failed. This is called to recover durable data and retry sends after a
   * crash.
   */
  public void recover() throws IOException;

  /**
   * Get a new sink to write events to. Events are durably written before being
   * transmitted. The tagger creates a unique name for the batch of events.
   */
  public EventSink newWritingSink(Tagger t) throws IOException;

  /**
   * Gets an unacked batch. Read from the Failover Event log by getting event
   * sources from the DiskFailoverManager. This changes state, and will block
   * when exhausted, or return null when shutdown.
   */
  public EventSource getUnsentSource() throws IOException;

  /**
   * Get a logical sink that breaks stream of data into mini batches
   * transparently to the user. When the RollTrigger's predicate condition is
   * met, the rolling sink closes the previous sink and calls newWritingSink to
   * roll to a new writer.
   */
  public RollSink getEventSink(Context ctx, RollTrigger t) throws IOException;

  /**
   * Get a source that logically provides a single stream. This should call
   * getUnackedSource() underneath to get another event batch.
   */
  public EventSource getEventSource() throws IOException;

  /**
   * This imports "external data" into the WAL. Currently this is used for
   * testing.
   * 
   * TODO (jon) This interface is not quite right -- it should take a file and a
   * format as an arg. This will be revisited when we revist the Log4J, Log4Net,
   * and avro serialization integration.
   */
  public void importData() throws IOException;

  /**
   * Transition a data set tagged with SENT to LOGGED for it to be retried.
   */
  public void retry(String tag) throws IOException;

  /**
   * Returns true if there dfo has no logged entries. This is useful for
   * completeness checking. (wait for after this is true before closing).
   */
  boolean isEmpty();

}
