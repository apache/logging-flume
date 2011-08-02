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

import com.cloudera.flume.master.StatusManager.NodeState;

/**
 * This is an edge that connect a source to a sink. This is an abstract class
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
   * Signals driver to stop. this does not block.
   */
  abstract public void stop() throws IOException;

  /**
   * This causes the driver to block until it is "done".
   */
  abstract public void join() throws InterruptedException;

  /**
   * return true if the driver has completed within specified ms. If it has not
   * completed by the specified time, it returns false.
   */
  abstract public boolean join(long ms) throws InterruptedException;

  abstract public NodeState getState();

  abstract public void registerListener(DriverListener listener);

  abstract public void deregisterListener(DriverListener listener);
}
