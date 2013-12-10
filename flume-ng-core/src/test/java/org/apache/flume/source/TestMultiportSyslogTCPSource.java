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
import com.google.common.collect.Lists;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.MultiportSyslogTCPSource.LineSplitter;
import org.apache.flume.source.MultiportSyslogTCPSource.MultiportSyslogHandler;
import org.apache.flume.source.MultiportSyslogTCPSource.ParsedBuffer;
import org.apache.flume.source.MultiportSyslogTCPSource.ThreadSafeDecoder;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.session.DefaultIoSessionDataStructureFactory;
import org.apache.mina.transport.socket.nio.NioSession;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.mockito.Mockito.*;

public class TestMultiportSyslogTCPSource {

  private static final Logger logger =
      LoggerFactory.getLogger(TestMultiportSyslogTCPSource.class);

  private static final int BASE_TEST_SYSLOG_PORT = 14455;
  private final DateTime time = new DateTime();
  private final String stamp1 = time.toString();
  private final String host1 = "localhost.localdomain";
  private final String data1 = "proc1 - some msg";
  private final static boolean KEEP_FIELDS = false;

  /**
   * Helper function to generate a syslog message.
   * @param counter
   * @return 
   */
  private byte[] getEvent(int counter) {
    // timestamp with 'Z' appended, translates to UTC
    String msg1 = "<10>" + stamp1 + " " + host1 + " " + data1 + " "
            + String.valueOf(counter) + "\n";
    return msg1.getBytes();
  }

  /**
   * Basic test to exercise multiple-port parsing.
   */
  @Test
  public void testMultiplePorts() throws IOException, ParseException {
    MultiportSyslogTCPSource source = new MultiportSyslogTCPSource();
    Channel channel = new MemoryChannel();

    Context channelContext = new Context();
    channelContext.put("capacity", String.valueOf(2000));
    channelContext.put("transactionCapacity", String.valueOf(2000));
    Configurables.configure(channel, channelContext);

    List<Channel> channels = Lists.newArrayList();
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));
    Context context = new Context();
    StringBuilder ports = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      ports.append(String.valueOf(BASE_TEST_SYSLOG_PORT + i)).append(" ");
    }
    context.put(SyslogSourceConfigurationConstants.CONFIG_PORTS,
        ports.toString().trim());
    source.configure(context);
    source.start();

    Socket syslogSocket;
    for (int i = 0; i < 1000 ; i++) {
      syslogSocket = new Socket(
              InetAddress.getLocalHost(), BASE_TEST_SYSLOG_PORT + i);
      syslogSocket.getOutputStream().write(getEvent(i));
      syslogSocket.close();
    }

    List<Event> channelEvents = new ArrayList<Event>();
    Transaction txn = channel.getTransaction();
    txn.begin();
    for (int i = 0; i < 1000; i++) {
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
    //Since events can arrive out of order, search for each event in the array
    for (int i = 0; i < 1000 ; i++) {
      Iterator<Event> iter = channelEvents.iterator();
      while (iter.hasNext()) {
        Event e = iter.next();
        Map<String, String> headers = e.getHeaders();
        // rely on port to figure out which event it is
        Integer port = null;
        if (headers.containsKey(
            SyslogSourceConfigurationConstants.DEFAULT_PORT_HEADER)) {
          port = Integer.parseInt(headers.get(
                SyslogSourceConfigurationConstants.DEFAULT_PORT_HEADER));
        }
        iter.remove();

        Assert.assertEquals("Timestamps must match",
            String.valueOf(time.getMillis()), headers.get("timestamp"));

        String host2 = headers.get("host");
        Assert.assertEquals(host1, host2);

        if (port != null) {
          int num = port - BASE_TEST_SYSLOG_PORT;
          Assert.assertEquals(data1 + " " + String.valueOf(num),
              new String(e.getBody()));
        }
      }
    }
    source.stop();
  }

  /**
   * Test the reassembly of a single line across multiple packets.
   */
  @Test
  public void testFragmented() throws CharacterCodingException {
    final int maxLen = 100;

    IoBuffer savedBuf = IoBuffer.allocate(maxLen);

    String origMsg = "<1>- - blah blam foo\n";
    IoBuffer buf1 = IoBuffer.wrap(
        origMsg.substring(0, 11).getBytes(Charsets.UTF_8));
    IoBuffer buf2 = IoBuffer.wrap(
        origMsg.substring(11, 16).getBytes(Charsets.UTF_8));
    IoBuffer buf3 = IoBuffer.wrap(
        origMsg.substring(16, 21).getBytes(Charsets.UTF_8));

    LineSplitter lineSplitter = new LineSplitter(maxLen);
    ParsedBuffer parsedLine = new ParsedBuffer();

    Assert.assertFalse("Incomplete line should not be parsed",
        lineSplitter.parseLine(buf1, savedBuf, parsedLine));
    Assert.assertFalse("Incomplete line should not be parsed",
        lineSplitter.parseLine(buf2, savedBuf, parsedLine));
    Assert.assertTrue("Completed line should be parsed",
        lineSplitter.parseLine(buf3, savedBuf, parsedLine));

    // the fragmented message should now be reconstructed
    Assert.assertEquals(origMsg.trim(),
        parsedLine.buffer.getString(Charsets.UTF_8.newDecoder()));
    parsedLine.buffer.rewind();

    MultiportSyslogTCPSource.MultiportSyslogHandler handler =
        new MultiportSyslogTCPSource.MultiportSyslogHandler(maxLen, 100, null,
        null, SyslogSourceConfigurationConstants.DEFAULT_PORT_HEADER,
        new ThreadSafeDecoder(Charsets.UTF_8),
        new ConcurrentHashMap<Integer, ThreadSafeDecoder>(),
        KEEP_FIELDS);

    Event event = handler.parseEvent(parsedLine, Charsets.UTF_8.newDecoder());
    String body = new String(event.getBody(), Charsets.UTF_8);
    Assert.assertEquals("Event body incorrect",
        origMsg.trim().substring(7), body);
  }

  /**
   * Test parser handling of different character sets.
   */
  @Test
  public void testCharsetParsing() throws FileNotFoundException, IOException {
    String header = "<10>2012-08-11T01:01:01Z localhost ";
    String enBody = "Yarf yarf yarf";
    String enMsg = header + enBody;
    String frBody = "Comment " + "\u00EA" + "tes-vous?";
    String frMsg = header + frBody;
    String esBody = "¿Cómo estás?";
    String esMsg = header + esBody;

    // defaults to UTF-8
    MultiportSyslogHandler handler = new MultiportSyslogHandler(
        1000, 10, new ChannelProcessor(new ReplicatingChannelSelector()),
        new SourceCounter("test"), "port",
        new ThreadSafeDecoder(Charsets.UTF_8),
        new ConcurrentHashMap<Integer, ThreadSafeDecoder>(),
        KEEP_FIELDS);

    ParsedBuffer parsedBuf = new ParsedBuffer();
    parsedBuf.incomplete = false;

    // should be able to encode/decode any of these messages in UTF-8 or ISO
    String[] bodies = { enBody, esBody, frBody };
    String[] msgs = { enMsg, esMsg, frMsg };
    Charset[] charsets = { Charsets.UTF_8, Charsets.ISO_8859_1 };
    for (Charset charset : charsets) {
      for (int i = 0; i < msgs.length; i++) {
        String msg = msgs[i];
        String body = bodies[i];
        parsedBuf.buffer = IoBuffer.wrap(msg.getBytes(charset));
        Event evt = handler.parseEvent(parsedBuf, charset.newDecoder());
        String result = new String(evt.getBody(), charset);
        // this doesn't work with non-UTF-8 chars... not sure why...
        Assert.assertEquals(charset + " parse error: " + msg, body, result);
        Assert.assertNull(
            evt.getHeaders().get(SyslogUtils.EVENT_STATUS));
      }
    }

    // Construct an invalid UTF-8 sequence.
    // The parser should still generate an Event, but mark it as INVALID.
    byte[] badUtf8Seq = enMsg.getBytes(Charsets.ISO_8859_1);
    int badMsgLen = badUtf8Seq.length;
    badUtf8Seq[badMsgLen - 2] = (byte)0xFE; // valid ISO-8859-1, invalid UTF-8
    badUtf8Seq[badMsgLen - 1] = (byte)0xFF; // valid ISO-8859-1, invalid UTF-8
    parsedBuf.buffer = IoBuffer.wrap(badUtf8Seq);
    Event evt = handler.parseEvent(parsedBuf, Charsets.UTF_8.newDecoder());
    Assert.assertEquals("event body: " +
        new String(evt.getBody(), Charsets.ISO_8859_1) +
        " and my default charset = " + Charset.defaultCharset() +
        " with event = " + evt,
        SyslogUtils.SyslogStatus.INVALID.getSyslogStatus(),
        evt.getHeaders().get(SyslogUtils.EVENT_STATUS));
    Assert.assertArrayEquals("Raw message data should be kept in body of event",
        badUtf8Seq, evt.getBody());

  }

  // helper function
  private static Event takeEvent(Channel channel) {
    Transaction txn = channel.getTransaction();
    txn.begin();
    Event evt = channel.take();
    txn.commit();
    txn.close();
    return evt;
  }

  /**
   * Test that different charsets are parsed by different ports correctly.
   */
  @Test
  public void testPortCharsetHandling() throws UnknownHostException, Exception {

    ///////////////////////////////////////////////////////
    // port setup

    InetAddress localAddr = InetAddress.getLocalHost();
    DefaultIoSessionDataStructureFactory dsFactory =
        new DefaultIoSessionDataStructureFactory();


    // one faker on port 10001
    int port1 = 10001;
    NioSession session1 = mock(NioSession.class);
    session1.setAttributeMap(dsFactory.getAttributeMap(session1));
    SocketAddress sockAddr1 = new InetSocketAddress(localAddr, port1);
    when(session1.getLocalAddress()).thenReturn(sockAddr1);

    // another faker on port 10002
    int port2 = 10002;
    NioSession session2 = mock(NioSession.class);
    session2.setAttributeMap(dsFactory.getAttributeMap(session2));
    SocketAddress sockAddr2 = new InetSocketAddress(localAddr, port2);
    when(session2.getLocalAddress()).thenReturn(sockAddr2);

    // set up expected charsets per port
    ConcurrentMap<Integer, ThreadSafeDecoder> portCharsets =
        new ConcurrentHashMap<Integer, ThreadSafeDecoder>();
    portCharsets.put(port1, new ThreadSafeDecoder(Charsets.ISO_8859_1));
    portCharsets.put(port2, new ThreadSafeDecoder(Charsets.UTF_8));

    ///////////////////////////////////////////////////////
    // channel / source setup

    // set up channel to receive events
    MemoryChannel chan = new MemoryChannel();
    chan.configure(new Context());
    chan.start();
    ReplicatingChannelSelector sel = new ReplicatingChannelSelector();
    sel.setChannels(Lists.<Channel>newArrayList(chan));
    ChannelProcessor chanProc = new ChannelProcessor(sel);

    // defaults to UTF-8
    MultiportSyslogHandler handler = new MultiportSyslogHandler(
        1000, 10, chanProc, new SourceCounter("test"), "port",
        new ThreadSafeDecoder(Charsets.UTF_8), portCharsets, KEEP_FIELDS);

    // initialize buffers
    handler.sessionCreated(session1);
    handler.sessionCreated(session2);

    ///////////////////////////////////////////////////////
    // event setup

    // Create events of varying charsets.
    String header = "<10>2012-08-17T02:14:00-07:00 192.168.1.110 ";

    // These chars encode under ISO-8859-1 as illegal bytes under UTF-8.
    String dangerousChars = "þÿÀÁ";

    ///////////////////////////////////////////////////////
    // encode and send them through the message handler
    String msg;
    IoBuffer buf;
    Event evt;

    // valid ISO-8859-1 on the right (ISO-8859-1) port
    msg = header + dangerousChars + "\n";
    buf = IoBuffer.wrap(msg.getBytes(Charsets.ISO_8859_1));
    handler.messageReceived(session1, buf);
    evt = takeEvent(chan);
    Assert.assertNotNull("Event vanished!", evt);
    Assert.assertNull(evt.getHeaders().get(SyslogUtils.EVENT_STATUS));

    // valid ISO-8859-1 on the wrong (UTF-8) port
    msg = header + dangerousChars + "\n";
    buf = IoBuffer.wrap(msg.getBytes(Charsets.ISO_8859_1));
    handler.messageReceived(session2, buf);
    evt = takeEvent(chan);
    Assert.assertNotNull("Event vanished!", evt);
    Assert.assertEquals("Expected invalid event due to character encoding",
        SyslogUtils.SyslogStatus.INVALID.getSyslogStatus(),
        evt.getHeaders().get(SyslogUtils.EVENT_STATUS));

    // valid UTF-8 on the right (UTF-8) port
    msg = header + dangerousChars + "\n";
    buf = IoBuffer.wrap(msg.getBytes(Charsets.UTF_8));
    handler.messageReceived(session2, buf);
    evt = takeEvent(chan);
    Assert.assertNotNull("Event vanished!", evt);
    Assert.assertNull(evt.getHeaders().get(SyslogUtils.EVENT_STATUS));
  }

}
