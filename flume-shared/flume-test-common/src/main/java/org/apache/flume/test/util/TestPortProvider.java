/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.test.util;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class TestPortProvider {
  public static final int CAPACITY = 50;
  private static final int MIN_SIZE = CAPACITY / 5;
  private static TestPortProvider instance;
  private static final Object lock = new Object();

  private BlockingQueue<ServerSocket> ports = new ArrayBlockingQueue<>(CAPACITY);

  private TestPortProvider() {
  }

  public static TestPortProvider getInstance() {
    if (instance == null) {
      synchronized (lock) {
        if (instance == null) {
          instance = new TestPortProvider();
        }
      }
    }

    return instance;
  }

  public synchronized Integer getFreePort() {
    ensureAvailablePorts();

    ServerSocket serverSocket = ports.poll();
    int port = serverSocket.getLocalPort();
    try {
      serverSocket.close();
    } catch (IOException e) {
      throw new IllegalStateException("Cannot free port " + port, e);
    }
    return port;
  }

  public synchronized List<Integer> getListOfFreePorts(int size) {
    List<Integer> portList = new ArrayList<>(size);
    if (size > ports.size()) {
      getServerSockets(size).forEach(serverSocket -> {
        int port = serverSocket.getLocalPort();
        portList.add(port);
        try {
          serverSocket.close();
        } catch (IOException e) {
          throw new IllegalStateException("Cannot free port " + port, e);
        }
      });
    } else {
      for (int i = 0; i < size; i++) {
        portList.add(getFreePort());
      }
    }

    return portList;
  }

  private void ensureAvailablePorts() {
    if (ports.size() < MIN_SIZE) {
      //clear free port pool to avoid collision
      ports.clear();
      ArrayList<ServerSocket> sockets = getServerSockets(CAPACITY - ports.size());

      for (ServerSocket next : sockets) {
        ports.offer(next);
        try {
          next.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

  }

  private ArrayList<ServerSocket> getServerSockets(int size) {
    ArrayList<ServerSocket> sockets = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      try {
        sockets.add(new ServerSocket(0));
      } catch (IOException e) {
        throw new IllegalStateException("Cannot open socket", e);
      }
    }

    return sockets;
  }
}