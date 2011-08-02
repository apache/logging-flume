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
package com.cloudera.flume.core;

import java.io.IOException;

/**
 * This is an edge that connects a source to a sink. This is an abstract class
 * because we can have many different properties for the connection between
 * them.
 * 
 * The default will be a direct connection - synchronous, single thread, single
 * stack.
 * 
 * Others could include a asynchronous versions that use a lifo or pqueue that
 * allows for multiple threads of consumers.
 */
abstract public class Driver {

  abstract public EventSource getSource();

  abstract public EventSink getSink();

  abstract public void setSource(EventSource src);

  abstract public void setSink(EventSink snk);

  /**
   * Signals driver to start. This blocks until the driver has started
   */
  abstract public void start() throws IOException;

  /**
   * Signals driver to do a stop that attempts to flush any internal buffers.
   * This does not block.
   */
  abstract public void stop() throws IOException;

  /**
   * Signals driver to do a abrupt shutdown. This does not guarantee that any
   * internal buffers will be flushed. This does not block.
   */
  abstract public void cancel();

  /**
   * This causes the driver to block until it is "done".
   */
  abstract public void join() throws InterruptedException;

  /**
   * return true if the driver has completed within specified ms. If it has not
   * completed by the specified time, it returns false. 0 ms means wait forever
   */
  abstract public boolean join(long ms) throws InterruptedException;

  /**
   * Block until transition to the specified state for at most millis ms.
   * 
   * @param state
   * @param millis
   * @return true if state transitioned to desired state, false if timeoed out.
   * @throws InterruptedException
   */
  abstract public boolean waitForState(DriverState state, long millis)
      throws InterruptedException;

  /**
   * Block until driver state to reaches at least the specified state where
   * HELLO < OPENING < ACTIVE < CLOSING < IDLE < ERROR or to millis ms has
   * elapsed.
   * 
   * @param state
   * @param millis
   * @return true if state transitioned to desired state, false if timeoed out.
   * @throws InterruptedException
   */
  abstract public boolean waitForAtLeastState(DriverState state, long millis)
      throws InterruptedException;

  /**
   * Driver state
   * 
   * WARNING: Changing the order of the enum will likely cause problems with
   * waitForAtLeastState.
   */
  public enum DriverState {
    HELLO, OPENING, ACTIVE, CLOSING, IDLE, ERROR
  };

  /**
   * Get the current state of the driver. (HELLO, OPENING, ACTIVE, CLOSING,
   * IDLE, ERROR)
   */
  abstract public DriverState getState();

  abstract public void registerListener(DriverListener listener);

  abstract public void deregisterListener(DriverListener listener);

}
