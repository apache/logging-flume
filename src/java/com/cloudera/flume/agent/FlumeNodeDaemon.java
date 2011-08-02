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
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a simple skeleton that controls a Flume node running service. This
 * was written for the Windows service but could be used for Unix daemon
 * controls as well. The function names should not be changed -- they are
 * specified by the Apache commons daemon wrapper program (prunsrv.exe renamed
 * to bin/flumenode.exe)
 */
public class FlumeNodeDaemon {
  private static final Logger LOG = LoggerFactory
      .getLogger(FlumeNodeDaemon.class);

  /**
   * Single static instance of the service class
   */
  private static final FlumeNodeDaemon serviceInstance = new FlumeNodeDaemon();
  private static FlumeNode node;

  /**
   * Static method called by prunsrv to start/stop the service. Pass the
   * argument "start" to start the service, and pass "stop" to stop the service.
   */
  public static void windowsService(String args[]) {
    String cmd = "start";
    if (args.length > 0) {
      cmd = args[0];
    }

    if ("start".equals(cmd)) {
      serviceInstance.start();
    } else {
      serviceInstance.stop();
    }
  }

  /**
   * Flag to know if this service instance has been stopped.
   */
  private CountDownLatch done;

  /**
   * Start this service instance
   */
  public void start() {
    if (node != null) {
      return; // TODO what supposed to happen if start called twice?
    }

    done = new CountDownLatch(1);

    try {
      node = FlumeNode.setup(new String[0]);
    } catch (IOException ioe) {
      LOG.error("IOException setting up node!", ioe);
    }

    // TODO What is supposed to happen if this fails? for now throw exn.
    if (node == null) {
      throw new RuntimeException("Failed to create node");
    }

    node.start();
    try {
      // start is assumed to run and block until "finished"
      done.await();
    } catch (InterruptedException e) {
      LOG.error("Daemon was interrupted", e);
    }
  }

  /**
   * Stop this service instance
   */
  public void stop() {
    node.stop();
    node = null;
    done.countDown();
  }
}
