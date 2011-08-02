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

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.handlers.rolling.RollTrigger;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.flume.reporter.Reportable;

/**
 * This is the interface for providing durability of events until the reach the
 * permanent store. This is intended for use as a write ahead log option that
 * requires an ack before data can be eliminated. Different implementations can
 * be encapsulated by this interface.
 * 
 * Implementations of this interface must be thread safe -- it may be called
 * from different threads.
 */
public interface WALManager extends Reportable, WALCompletionNotifier {
  /**
   * These are the states a batch of events can be in.
   * 
   * IMPORT means external to the main flow of data.
   * 
   * WRITING means that there is some entity writing to the set of data.
   * 
   * LOGGED means that the data is durable on the node.
   * 
   * SENDING means that data is durable and is being sent somewhere downstream
   * 
   * SENT means that data has been received by the next downstream node.
   * 
   * E2EACKED means that data has been received and processed by the end point
   * node.
   * 
   * ERROR means that the set of data contains an error.
   */
  enum State {
    IMPORT, WRITING, LOGGED, SENDING, SENT, E2EACKED, ERROR
  };

  final public static String A_IMPORTED = "imported";
  final public static String A_WRITING = "writing";
  final public static String A_LOGGED = "logged";
  final public static String A_SENDING = "sending";
  final public static String A_SENT = "sent";
  final public static String A_ACKED = "acked";
  final public static String A_RETRY = "retry";
  final public static String A_ERROR = "error";
  final public static String A_RECOVERED = "recovered";
  final public static String A_IN_LOGGED = "loggedQ";
  final public static String A_IN_SENT = "sentQ";

  /**
   * Open the WAL manager to enable reads and writes.
   */
  public void open() throws IOException;

  /**
   * Closes the WAL manager for reads and writes
   */
  public void stopDrains() throws IOException;

  /**
   * Recover data that is not in E2EACKED or RROR state assuming that data
   * transmission has failed. This is called to recover durable data and retry
   * sends after a crash.
   */
  public void recover() throws IOException;

  /**
   * Get a new sink to write events to. Events are durably written before being
   * transmitted. The tagger creates a unique name for the batch of events.
   */
  public EventSink newWritingSink(Tagger t) throws IOException;

  /**
   * Gets an unacked batch. Read from the WAL by getting event sources from the
   * WAL manager.
   */
  public EventSource getUnackedSource() throws IOException, InterruptedException;

  /**
   * Get a logical sink that breaks stream of data into mini batches
   * transparently to the user. When the RollTrigger's predicate condition is
   * met, the rolling sink closes the previous sink and calls newWritingSink to
   * roll to a new writer.
   * 
   * An ackQueue listener reference is passed to provide callback hooks that are
   * called on different rolling sink transitions. (open, error, close).
   */
  public RollSink getAckingSink(Context ctx, RollTrigger t,
      final AckListener ackQueue, long checkMs) throws IOException;

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
   * format as an arg. This will be revisited when we revisit the Log4J, Log4Net,
   * and Avro serialization integration.
   */
  public void importData() throws IOException;

  /**
   * Returns true if the WAL has no logged entries. This is useful for
   * completeness checking. (wait for after this is true before closing).
   */
  boolean isEmpty();

}
