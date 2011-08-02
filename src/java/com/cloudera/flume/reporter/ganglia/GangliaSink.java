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

/*
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

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics.spi.Util;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.Attributes.Type;
import com.cloudera.util.NetUtils;
import com.google.common.base.Preconditions;

/**
 * This sink takes a specified list of ganglia gmond servers, a event attribute
 * (from a report/event), a units name, and a Flume attribute type, and emits
 * XDR formatted UDP packets so that the metric can be seen in Ganglia. This
 * specifically supports the ganglia >=3.1.x wire format.
 * 
 * Originally based on o.a.h.metrics.ganglia.GangliaContext31 / HADOOP-4675
 * 
 * XDR spec http://www.faqs.org/rfcs/rfc1832.html
 * 
 * This is not thread safe.
 */
public class GangliaSink extends EventSink.Base {

  private static final String DEFAULT_GROUP = "flume";
  private static final int DEFAULT_TMAX = 60;
  private static final int DEFAULT_DMAX = 0;
  private static final int DEFAULT_PORT = 8649;
  private static final int BUFFER_SIZE = 1500; // as per libgmond.c

  private final Log LOG = LogFactory.getLog(this.getClass());
  private final String servers;

  private static final Map<Type, String> typeTable =
      new HashMap<Type, String>() {
        private static final long serialVersionUID = 1L;
        {
          put(Type.STRING, "string");
          put(Type.INT, "int32");
          put(Type.LONG, "float"); // Conversion happens here, originally from
          // hadoop source
          put(Type.DOUBLE, "double");
        }
      };

  private byte[] buffer = new byte[BUFFER_SIZE];
  private int offset;

  private List<? extends SocketAddress> metricsServers;

  private DatagramSocket datagramSocket;
  final private String attr; // turns into the metric name.
  final private String units;
  final private Type type;

  /** Creates a new instance of GangliaContext */
  public GangliaSink(String gangliaSvrs, String attr, String units, Type t) {
    this.servers = gangliaSvrs;
    this.attr = attr;
    this.units = units;
    this.type = t;
  }

  /**
   * This currently only outputs one metric from a report, and relies on the
   * sink constructor to give details about the typing information.
   * 
   * This may become moot if typing information is provided by flume's data.
   */
  @Override
  public void append(Event e) throws IOException {
    String value;
    switch (type) {
    case LONG: {
      Long l = Attributes.readLong(e, attr);
      if (l == null) {
        // attribute not present, drop
        return;
      }
      value = l.toString();
      break;
    }
    case INT: {
      Integer i = Attributes.readInt(e, attr);
      if (i == null) {
        // attribute not present, drop
        return;
      }
      value = i.toString();
      break;
    }
    case STRING: {
      String s = Attributes.readString(e, attr);
      if (s == null) {
        // attribute not present, drop
        return;
      }
      value = s;
      break;
    }
    case DOUBLE: {
      Double d = Attributes.readDouble(e, attr);
      if (d == null) {
        // attribute not present,drop
        return;
      }
      value = d.toString();
      break;
    }
    default:
      return;
    }
    emitMetric(attr, typeTable.get(type), value, units);
  }

  @Override
  public void open() throws IOException {
    // TODO (jon) need to worry about SecurityException and other
    // RuntimeExceptions.

    // From o.a.h.metrics.spi.Util
    // This can throw IllegalArgumentException or SecurityException
    metricsServers = Util.parse(servers, DEFAULT_PORT);

    try {
      datagramSocket = new DatagramSocket();
    } catch (SocketException se) {
      LOG.warn("problem with ganglia socket", se);
    }

  }

  /**
   * Not thread safe
   */
  @Override
  public void close() throws IOException {
    if (datagramSocket == null) {
      LOG.warn("Double close");
      return;
    }
    datagramSocket.close();
    datagramSocket = null;
  }

  // //////////////////////////////////////////////////////////////////
  // This is ganglia >= 3.1.x wire format. Basically ripped out of
  // HADOOP-4675

  /**
   * This takes a metric and then send it off to the listening ganglia gmond
   * ports via udp.
   * 
   * This is not thread safe.
   */
  private void emitMetric(String name, String type, String value, String units)
      throws IOException {
    int slope = 3; // see gmetric.c
    int tmax = DEFAULT_TMAX;
    int dmax = DEFAULT_DMAX;
    String hostName = NetUtils.localhost();

    // First we send out a metadata message
    xdr_int(128); // metric_id = metadata_msg
    xdr_string(hostName); // hostname
    xdr_string(name); // metric name
    xdr_int(0); // spoof = False
    xdr_string(type); // metric type
    xdr_string(name); // metric name
    xdr_string(units); // units
    xdr_int(slope); // slope
    xdr_int(tmax); // tmax, the maximum time between metrics
    xdr_int(dmax); // dmax, the maximum data value
    xdr_int(1); // ???

    /*
     * Num of the entries in extra_value field for Ganglia 3.1.x
     */
    xdr_string("GROUP"); /* Group attribute */
    xdr_string(DEFAULT_GROUP); /* Group value */

    // send to each ganglia metrics listener/server
    for (SocketAddress socketAddress : metricsServers) {
      DatagramPacket packet = new DatagramPacket(buffer, offset, socketAddress);
      datagramSocket.send(packet);
    }

    // Now we send out a message with the actual value.
    // Technically, we only need to send out the metadata message once for
    // each metric, but I don't want to have to record which metrics we did and
    // did not send.
    offset = 0;
    xdr_int(133); // we are sending a string value
    xdr_string(hostName); // hostName
    xdr_string(name); // metric name
    xdr_int(0); // spoof = False
    xdr_string("%s"); // format field
    xdr_string(value); // metric value

    for (SocketAddress socketAddress : metricsServers) {
      DatagramPacket packet = new DatagramPacket(buffer, offset, socketAddress);
      datagramSocket.send(packet);
    }

  }

  /**
   * Puts a string into the buffer by first writing the size of the string as an
   * int, followed by the bytes of the string, padded if necessary to a multiple
   * of 4.
   */
  private void xdr_string(String s) {
    byte[] bytes = s.getBytes();
    int len = bytes.length;
    xdr_int(len);
    System.arraycopy(bytes, 0, buffer, offset, len);
    offset += len;
    pad();
  }

  /**
   * Pads the buffer with zero bytes up to the nearest multiple of 4.
   * 
   * Not thread safe
   */
  private void pad() {
    int newOffset = ((offset + 3) / 4) * 4;
    while (offset < newOffset) {
      buffer[offset++] = 0;
    }
  }

  /**
   * Puts an integer into the buffer as 4 bytes, big-endian.
   * 
   * Not thread safe
   */
  private void xdr_int(int i) {
    buffer[offset++] = (byte) ((i >> 24) & 0xff);
    buffer[offset++] = (byte) ((i >> 16) & 0xff);
    buffer[offset++] = (byte) ((i >> 8) & 0xff);
    buffer[offset++] = (byte) (i & 0xff);
  }

  // end of rip
  // //////////////////////////////////////////////////////////////////

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length >= 3 && argv.length <= 4,
            "usage: ganglia(\"attr\", \"units\",\"type\"[, \"gmondservers\"])");

        String ganglias = FlumeConfiguration.get().getGangliaServers(); // default
        String attr = argv[0];
        String units = argv[1];

        String type = argv[2];
        Type t = null;
        // These strings are flume types, not xdr types
        if (type.equals("int")) {
          t = Type.INT;
        } else if (type.equals("long")) {
          t = Type.LONG;
        } else if (type.equals("double")) {
          t = Type.DOUBLE;
        } else if (type.equals("string")) {
          t = Type.STRING;
        } else {
          throw new IllegalArgumentException(
              "Illegal ganglia xdr type: "
                  + type
                  + " != int|long|double|string\n"
                  + "usage: ganglia(\"attr\", \"units\",\"type\"[, \"gmondservers\"]");
        }

        if (argv.length >= 4) {
          ganglias = argv[3];
        }
        EventSink snk = new GangliaSink(ganglias, attr, units, t);
        return snk;

      }
    };
  }
}
