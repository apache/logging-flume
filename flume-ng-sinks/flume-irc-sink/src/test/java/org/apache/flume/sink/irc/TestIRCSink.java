/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.flume.sink.irc;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.flume.*;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.fail;

public class TestIRCSink {

  private File eventFile;
  int ircServerPort;
  DumbIRCServer dumbIRCServer;
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private static int findFreePort() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  @Before
  public void setUp() throws IOException {
    ircServerPort = findFreePort();
    dumbIRCServer = new DumbIRCServer(ircServerPort);
    dumbIRCServer.start();
    eventFile = folder.newFile("eventFile.txt");
  }

  @After
  public void tearDown() throws Exception {
    dumbIRCServer.shutdownServer();
  }

  @Test
  public void testIRCSinkMissingSplitLineProperty() {
    Sink ircSink = new IRCSink();
    ircSink.setName("IRC Sink - " + UUID.randomUUID().toString());
    Context context = new Context();
    context.put("hostname", "localhost");
    context.put("port", String.valueOf(ircServerPort));
    context.put("nick", "flume");
    context.put("password", "flume");
    context.put("user", "flume");
    context.put("name", "flume-dev");
    context.put("chan", "flume");
    context.put("splitchars", "false");
    Configurables.configure(ircSink, context);
    Channel memoryChannel = new MemoryChannel();
    Configurables.configure(memoryChannel, context);
    ircSink.setChannel(memoryChannel);
    ircSink.start();
    Transaction txn = memoryChannel.getTransaction();
    txn.begin();
    Event event = EventBuilder.withBody("Dummy Event".getBytes());
    memoryChannel.put(event);
    txn.commit();
    txn.close();
    try {
      Sink.Status status = ircSink.process();
      if (status == Sink.Status.BACKOFF) {
        fail("Error occured");
      }
    } catch (EventDeliveryException eDelExcp) {
      // noop
    }
  }

  class DumbIRCServer extends Thread {
    int port;
    ServerSocket ss;

    public DumbIRCServer(int port) {
      this.port = port;
    }

    public void run() {
      try {
        ss = new ServerSocket(port);
        while (true) {
          try {
            Socket socket = ss.accept();
            process(socket);
          } catch (Exception ex) {/* noop */ }
        }
      } catch (IOException e) {
        // noop
      }
    }

    public void shutdownServer() throws Exception {
      ss.close();
    }

    /**
     * Process the incoming request from IRC client
     *
     * @param socket  IRC client connection socket
     * @throws IOException
     */
    private void process(Socket socket) throws IOException {
      FileOutputStream fileOutputStream = FileUtils.openOutputStream(eventFile);
      List<String> input = IOUtils.readLines(socket.getInputStream());
      for (String next : input) {
        if (isPrivMessage(next)) {
          fileOutputStream.write(next.getBytes());
          fileOutputStream.write("\n".getBytes());
        }
      }
      fileOutputStream.close();
      socket.close();
    }

    /**
     * Checks if the message is Priv message
     *
     * @param input command received from IRC client
     * @return true, if command received is PrivMessage
     */
    private boolean isPrivMessage(String input) {
      return input.startsWith("PRIVMSG");
    }
  }
}