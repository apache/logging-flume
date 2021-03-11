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
package org.apache.flume.source;

import com.google.common.base.Charsets;
import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class TestSyslogTcpSource {
  private static final org.slf4j.Logger logger =
      LoggerFactory.getLogger(TestSyslogTcpSource.class);
  private static final String TEST_CLIENT_IP_HEADER = "testClientIPHeader";
  private static final String TEST_CLIENT_HOSTNAME_HEADER = "testClientHostnameHeader";

  private SyslogTcpSource source;
  private Channel channel;
  private static final int TEST_SYSLOG_PORT = 0;
  private final DateTime time = new DateTime();
  private final String stamp1 = time.toString();
  private final String host1 = "localhost.localdomain";
  private final String data1 = "test syslog data";
  private final String bodyWithHostname = host1 + " " +
      data1;
  private final String bodyWithTimestamp = stamp1 + " " +
      data1;
  private final String bodyWithTandH = "<10>" + stamp1 + " " + host1 + " " +
      data1 + "\n";

  private void init(String keepFields) {
    init(keepFields, new Context());
  }

  private void init(String keepFields, Context context) {
    source = new SyslogTcpSource();
    channel = new MemoryChannel();

    Configurables.configure(channel, new Context());

    List<Channel> channels = new ArrayList<>();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));

    context.put("host", InetAddress.getLoopbackAddress().getHostAddress());
    context.put("port", String.valueOf(TEST_SYSLOG_PORT));
    context.put("keepFields", keepFields);

    source.configure(context);

  }

  private void initSsl() {
    Context context = new Context();
    context.put("ssl", "true");
    context.put("keystore", "src/test/resources/server.p12");
    context.put("keystore-password", "password");
    context.put("keystore-type", "PKCS12");
    init("none", context);
  }

  /** Tests the keepFields configuration parameter (enabled or disabled)
   using SyslogTcpSource.*/
  private void runKeepFieldsTest(String keepFields) throws IOException {
    init(keepFields);
    source.start();
    // Write some message to the syslog port
    InetSocketAddress addr = source.getBoundAddress();
    for (int i = 0; i < 10 ; i++) {
      try (Socket syslogSocket = new Socket(addr.getAddress(), addr.getPort())) {
        syslogSocket.getOutputStream().write(bodyWithTandH.getBytes());
      }
    }

    List<Event> channelEvents = new ArrayList<>();
    Transaction txn = channel.getTransaction();
    txn.begin();
    for (int i = 0; i < 10; i++) {
      Event e = channel.take();
      if (e == null) {
        throw new NullPointerException("Event is null");
      }
      channelEvents.add(e);
    }

    try {
      txn.commit();
    } catch (Throwable t) {
      txn.rollback();
    } finally {
      txn.close();
    }

    source.stop();
    for (Event e : channelEvents) {
      Assert.assertNotNull(e);
      String str = new String(e.getBody(), Charsets.UTF_8);
      logger.info(str);
      if (keepFields.equals("true") || keepFields.equals("all")) {
        Assert.assertArrayEquals(bodyWithTandH.trim().getBytes(),
            e.getBody());
      } else if (keepFields.equals("false") || keepFields.equals("none")) {
        Assert.assertArrayEquals(data1.getBytes(), e.getBody());
      } else if (keepFields.equals("hostname")) {
        Assert.assertArrayEquals(bodyWithHostname.getBytes(), e.getBody());
      } else if (keepFields.equals("timestamp")) {
        Assert.assertArrayEquals(bodyWithTimestamp.getBytes(), e.getBody());
      }
    }
  }

  @Test
  public void testKeepFields() throws IOException {
    runKeepFieldsTest("all");

    // Backwards compatibility
    runKeepFieldsTest("true");
  }

  @Test
  public void testRemoveFields() throws IOException {
    runKeepFieldsTest("none");

    // Backwards compatibility
    runKeepFieldsTest("false");
  }

  @Test
  public void testKeepHostname() throws IOException {
    runKeepFieldsTest("hostname");
  }

  @Test
  public void testKeepTimestamp() throws IOException {
    runKeepFieldsTest("timestamp");
  }

  @Test
  public void testSourceCounter() throws IOException {
    runKeepFieldsTest("all");
    assertEquals(10, source.getSourceCounter().getEventAcceptedCount());
    assertEquals(10, source.getSourceCounter().getEventReceivedCount());
  }

  @Test
  public void testSourceCounterChannelFail() throws Exception {
    init("true");

    errorCounterCommon(new ChannelException("dummy"));

    for (int i = 0; i < 10 && source.getSourceCounter().getChannelWriteFail() == 0; i++) {
      Thread.sleep(100);
    }
    assertEquals(1, source.getSourceCounter().getChannelWriteFail());
  }

  @Test
  public void testSourceCounterEventFail() throws Exception {
    init("true");

    errorCounterCommon(new RuntimeException("dummy"));

    for (int i = 0; i < 10 && source.getSourceCounter().getEventReadFail() == 0; i++) {
      Thread.sleep(100);
    }
    assertEquals(1, source.getSourceCounter().getEventReadFail());
  }

  private void errorCounterCommon(Exception e) throws IOException {
    ChannelProcessor cp = Mockito.mock(ChannelProcessor.class);
    doThrow(e).when(cp).processEvent(any(Event.class));
    source.setChannelProcessor(cp);

    source.start();
    // Write some message to the syslog port
    InetSocketAddress addr = source.getBoundAddress();
    try (Socket syslogSocket = new Socket(addr.getAddress(), addr.getPort())) {
      syslogSocket.getOutputStream().write(bodyWithTandH.getBytes());
    }
  }

  @Test
  public void testSSLMessages() throws Exception {
    initSsl();

    source.start();
    InetSocketAddress address = source.getBoundAddress();

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, new TrustManager[]{new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] certs, String s) {
          // nothing
        }

        @Override
        public void checkServerTrusted(X509Certificate[] certs, String s) {
          // nothing
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return new X509Certificate[0];
        }
      } },
        null);
    SocketFactory socketFactory = sslContext.getSocketFactory();
    Socket socket = socketFactory.createSocket();
    socket.connect(address);
    OutputStream outputStream = socket.getOutputStream();
    outputStream.write(bodyWithTandH.getBytes());
    socket.close();
   // Thread.sleep(100);
    Transaction transaction = channel.getTransaction();
    transaction.begin();

    Event event = channel.take();
    assertEquals(new String(event.getBody()), data1);
    transaction.commit();
    transaction.close();

  }


  @Test
  public void testClientHeaders() throws IOException {
    String testClientIPHeader = "testClientIPHeader";
    String testClientHostnameHeader = "testClientHostnameHeader";

    Context context = new Context();
    context.put("clientIPHeader", testClientIPHeader);
    context.put("clientHostnameHeader", testClientHostnameHeader);

    init("none", context);

    source.start();
    // Write some message to the syslog port
    InetSocketAddress addr = source.getBoundAddress();
    Socket syslogSocket = new Socket(addr.getAddress(), addr.getPort());
    syslogSocket.getOutputStream().write(bodyWithTandH.getBytes());

    Transaction txn = channel.getTransaction();
    txn.begin();
    Event e = channel.take();

    try {
      txn.commit();
    } catch (Throwable t) {
      txn.rollback();
    } finally {
      txn.close();
    }

    source.stop();

    Map<String, String> headers = e.getHeaders();

    InetAddress loopbackAddress = InetAddress.getLoopbackAddress();
    checkHeader(headers, TEST_CLIENT_IP_HEADER, loopbackAddress.getHostAddress());
    checkHeader(headers, TEST_CLIENT_HOSTNAME_HEADER, loopbackAddress.getHostName());
  }

  private static void checkHeader(Map<String, String> headers, String headerName,
      String expectedValue) {
    assertTrue("Missing event header: " + headerName, headers.containsKey(headerName));


    String headerValue = headers.get(headerName);
    if (TEST_CLIENT_HOSTNAME_HEADER.equals(headerName)) {
      if (!"localhost".equals(headerValue) && !"127.0.0.1".equals(headerValue)) {
        fail("Expected either 'localhost' or '127.0.0.1'");
      }
    } else {
      assertEquals("Event header value does not match: " + headerName,
              expectedValue, headerValue);
    }
  }
}

