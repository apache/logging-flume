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

package com.cloudera.flume.reporter.ganglia;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import org.apache.log4j.Logger;
import org.junit.Ignore;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.FanOutSink;
import com.cloudera.flume.core.Attributes.Type;
import com.cloudera.util.Clock;
import com.cloudera.util.NetUtils;

/**
 * This is a test of the quick and dirty ganglia integration.
 */
public class TestGangliaSink {
  private static final Logger LOG = Logger.getLogger(TestGangliaSink.class);

  // TODO (jon) Parens are invalid in metric names. (what other chars are
  // illegal in XDR Strings?
  String ATTR_LONG = "long test metric";
  String ATTR_INT = "int test metric";
  String ATTR_STRING = "string test metric";
  String ATTR_DOUBLE = "double test metric";

  /**
   * This test doesn't have a check, instead just it sends bogus data to an
   * actual ganglia gmond. This needs a human needs to verify.
   */
  @Ignore("Slow test, requires human to verify that values show up in ganglia")
  @Test
  public void sendDatatypesToGanglia_3_1_x() throws IOException,
      InterruptedException {
    // assumes there is a ganglia on local host
    // since using udp doesn't matter if it gets there or not.

    // default ganglia gmond multicast destination ip.
    String svrs = "239.2.11.71";

    EventSink lsnk = new GangliaSink(svrs, ATTR_LONG, "bytes", Type.LONG);
    EventSink isnk = new GangliaSink(svrs, ATTR_INT, "bytes", Type.INT);
    EventSink dsnk = new GangliaSink(svrs, ATTR_DOUBLE, "bytes", Type.DOUBLE);
    EventSink snk = new FanOutSink<EventSink>(lsnk, isnk, dsnk);
    snk.open();
    // This is enough for the data to register.
    for (int i = 0; i < 60; i++) {
      Event e = new EventImpl("".getBytes());
      Attributes.setLong(e, ATTR_LONG, i * 1000000);
      Attributes.setInt(e, ATTR_INT, (int) (i * 1000000));
      Attributes.setDouble(e, ATTR_DOUBLE, (double) (1.0 / (i % 20)));
      snk.append(e);
      Clock.sleep(1000);
    }
    snk.close();

  }

  @Test
  public void testBuilder() throws IOException {
    EventSink snk =
        GangliaSink.builder().build(new Context(), "localhost", "foo", "int");
    for (int i = 0; i < 10; i++) {
      snk.open();
      snk.append(new EventImpl("".getBytes()));
      snk.close();
    }

    EventSink snk4 =
        GangliaSink.builder().build(new Context(), "localhost", "foo", "int",
            FlumeConfiguration.get().getGangliaServers());
    for (int i = 0; i < 10; i++) {
      snk4.open();
      snk4.append(new EventImpl("".getBytes()));
      snk4.close();
    }

    try {
      GangliaSink.builder().build(new Context(), "localhost", "foo", "bar");
    } catch (IllegalArgumentException e) {
      // expected a bad type ;
      return;
    }
    fail("expected failure");

  }

  @Test
  public void testFactoryBuild() throws FlumeSpecException, IOException {
    EventSink snk =
        new CompositeSink(new Context(),
            "ganglia(\"localhost\", \"foo\", \"int\")");
    for (int i = 0; i < 10; i++) {
      snk.open();
      snk.append(new EventImpl("".getBytes()));
      snk.close();
    }

  }

  // //////////////////////////////////////////////////////////////////
  // This is ganglia >= 3.1.x wire format. Basically ripped out of
  // HADOOP-4675

  /**
   * This class is a runnable that will listen for Ganglia connections.
   */
  class GangliaSocketListener implements Runnable {

    private boolean isConfigured = false;
    private boolean hasData = false;
    private byte[] byteData;
    private int port;

    public void run() {
      DatagramSocket s;
      try {
        s = new DatagramSocket();
        setPort(s.getLocalPort());
        setConfigured(true);
      } catch (IOException e) {
        LOG.warn(e);
        synchronized (this) {
          this.notify();
        }
        return;
      }

      byte[] b = new byte[8192];
      DatagramPacket info = new DatagramPacket(b, b.length);

      synchronized (this) {
        this.notify();
      }
      try {
        s.receive(info);
      } catch (IOException e) {
        LOG.warn(e);
        synchronized (this) {
          this.notify();
        }
        return;
      }
      LOG.info("Got a new packet, length " + info.getLength());
      int bytesRead = info.getLength();
      if (bytesRead > 0)
        setHasData(true);

      byteData = new byte[info.getLength()];
      System.arraycopy(info.getData(), 0, byteData, 0, bytesRead);
      synchronized (this) {
        this.notify();
      }
    }

    public void setConfigured(boolean isConfigured) {
      this.isConfigured = isConfigured;
    }

    public boolean getConfigured() {
      return isConfigured;
    }

    public void setHasData(boolean hasData) {
      this.hasData = hasData;
    }

    public boolean getHasData() {
      return hasData;
    }

    public byte[] getBytes() {
      return byteData;
    }

    public void setPort(int port) {
      this.port = port;
    }

    public int getPort() {
      return port;
    }

  }

  /**
   * This test is stolen and hacked from hadoop's TestGangliaContext31
   */
  @Test
  public void testGanglia31Metrics() throws IOException {

    String hostName = NetUtils.localhost();
    GangliaSocketListener listener = new GangliaSocketListener();
    Thread listenerThread = new Thread(listener);
    listenerThread.start();
    try {
      synchronized (listener) {
        listener.wait();
      }
    } catch (InterruptedException e) {
      LOG.warn(e);
    }

    assertTrue("Could not configure the socket listener for Ganglia", listener
        .getConfigured());

    LOG.info("Listening to port " + listener.getPort());

    // setup and send some ganglia data.
    EventSink ganglia =
        new GangliaSink(hostName + ":" + listener.getPort(), "foo", "bars",
            Type.INT);
    ganglia.open();
    Event e = new EventImpl("baz".getBytes());
    Attributes.setInt(e, "foo", 1337);
    ganglia.append(e);
    ganglia.close();

    try {
      if (!listener.getHasData())
        synchronized (listener) {
          listener.wait(5 * 1000); // Wait at most 5 seconds for Ganglia data
        }
    } catch (InterruptedException ex) {
      LOG.warn(ex);
    }
    assertTrue("Did not recieve Ganglia data", listener.getHasData());

    byte[] hostNameBytes = hostName.getBytes();

    byte[] xdrBytes = listener.getBytes();

    // Try to make sure that the received bytes from Ganglia has the correct
    // hostname for this host
    boolean hasHostname = false;
    LOG.info("Checking to make sure that the Ganglia data contains host "
        + hostName);
    for (int i = 0; i < xdrBytes.length - hostNameBytes.length; i++) {
      hasHostname = true;
      for (int j = 0; j < hostNameBytes.length; j++) {
        if (xdrBytes[i + j] != hostNameBytes[j]) {
          hasHostname = false;
          break;
        }
      }
      if (hasHostname)
        break;
    }
    assertTrue("Did not correctly resolve hostname in Ganglia", hasHostname);
  }
  // end of rip
  // //////////////////////////////////////////////////////////////////

}
