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

import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class TestPortProviderTest {

  @Test
  public void getInstanceReturnsWithInstance() throws Exception {
    TestPortProvider instance = TestPortProvider.getInstance();
    assertThat(instance, instanceOf(TestPortProvider.class));
  }

  @Test
  public void getInstanceReturnsWithTheSameInstanceSecondTime() throws Exception {
    TestPortProvider instance1 = TestPortProvider.getInstance();
    TestPortProvider instance2 = TestPortProvider.getInstance();
    assertEquals(instance1, instance2);
  }

  @Test
  public void getFreePortReturnsAFreePort() throws Exception {
    Integer freePort = TestPortProvider.getInstance().getFreePort();
    ServerSocket serverSocket = new ServerSocket(freePort);
    assertTrue(serverSocket.isBound());
  }

  @Test
  public void getListOfFreePorts() throws Exception {
    List<Integer> freePorts = TestPortProvider.getInstance().getListOfFreePorts(10);
    List<ServerSocket> testSockets = new ArrayList<>(freePorts.size());
    testPortList(freePorts, testSockets);
  }

  @Test
  public void getListOfFreePortsWithLargeNumber() throws Exception {
    List<Integer> freePorts = TestPortProvider.getInstance().getListOfFreePorts(1000);
    List<ServerSocket> testSockets = new ArrayList<>(freePorts.size());
    testPortList(freePorts, testSockets);
  }

  private void testPortList(List<Integer> freePorts, List<ServerSocket> testSockets)
      throws IOException {
    for (int freePort : freePorts) {
      ServerSocket serverSocket = new ServerSocket(freePort);
      assertTrue(serverSocket.isBound());
      testSockets.add(serverSocket);
    }
    for (ServerSocket serverSocket : testSockets) {
      serverSocket.close();
    }
  }
}