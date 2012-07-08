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
package com.cloudera.flume.handlers.scribe;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import org.junit.Test;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;

/**
 * Test the scribe event source which runs over thrift
 */
public class TestScribeSource {

  public static final Logger LOG = LoggerFactory.getLogger(TestScribeSource.class);

  /**
   * Test that events can be sent and received, and that the correct metadata is
   * extracted.
   */
  @Test
  public void testScribeEventSourceAPI() throws IOException, TException,
      InterruptedException {
    ScribeEventSource src = new ScribeEventSource();
    src.open();

    // Open the client connection
    TTransport transport = new TSocket("localhost", FlumeConfiguration.get()
        .getScribeSourcePort());
    // scribe clients used framed transports
    transport = new TFramedTransport(transport);
    // scribe clients do not use strict write
    TProtocol protocol = new TBinaryProtocol(transport, false, false);
    transport.open();
    scribe.Client client = new scribe.Client(protocol);

    // Note - there is a tiny possibility of a race here, which is why we retry
    for (int i = 0; i < 3; ++i) {
      if (client.getStatus() != fb_status.ALIVE) {
        Thread.sleep(500);
      } else {
        break;
      }
    }
    assertEquals("ScribeEventSource did not come up in time!", fb_status.ALIVE,
        client.getStatus());

    LogEntry l1 = new LogEntry("mycategory", "mymessage");
    List<LogEntry> logs = new ArrayList<LogEntry>();
    logs.add(l1);
    client.Log(logs);

    Event e = src.next();

    src.close();

    assertEquals("mymessage", new String(e.getBody()), "mymessage");
    assertEquals("mycategory", new String(e.getAttrs().get(
        ScribeEventSource.SCRIBE_CATEGORY)));
  }

  @Test
  public void testOpenClose() throws IOException, TException,
      InterruptedException {
    EventSource src = ScribeEventSource.builder().build("45872");
    for (int i = 0; i < 10; ++i) {
      src.open();
      src.close();
    }
    src.open();

    // Open the client connection
    TTransport transport = new TSocket("localhost", 45872);
    transport = new TFramedTransport(transport);
    // scribe clients do not use strict write
    TProtocol protocol = new TBinaryProtocol(transport, false, false);
    transport.open();
    scribe.Client client = new scribe.Client(protocol);

    // Note - there is a tiny possibility of a race here, which is why we retry
    for (int i = 0; i < 3; ++i) {
      if (client.getStatus() != fb_status.ALIVE) {
        Thread.sleep(500);
      } else {
        break;
      }
    }
    assertEquals("ScribeEventSource did not come up in time!", fb_status.ALIVE,
        client.getStatus());
    src.close();
  }

  /**
   * This test starts a scribe source and blocks on next in one thread and then
   * attempt to close it from another. If the thread doesn't return the test
   * will timeout and fail.
   */
  @Test
  public void testConcurrentClose() throws InterruptedException, IOException {
    final CountDownLatch started = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(1);
    final ScribeEventSource src = new ScribeEventSource();

    new Thread() {
      @Override
      public void run() {
        try {
          src.open();
          started.countDown();
          src.next();
          done.countDown();
        } catch (IOException e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }.start();

    assertTrue("Open timed out", started.await(5, TimeUnit.SECONDS));
    src.close();
    assertTrue("Next timed out", done.await(5, TimeUnit.SECONDS));

  }
}
