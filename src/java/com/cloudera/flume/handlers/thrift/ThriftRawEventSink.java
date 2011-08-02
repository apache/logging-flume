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
package com.cloudera.flume.handlers.thrift;

import java.io.IOException;

import org.apache.thrift.TException;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.hdfs.WriteableEvent;

/**
 * This is a sink that will send events to a remote host/port. Instead of using
 * a ThriftEventAdaptor to convert to a ThriftFlumeEvent, we serialize the event
 * using its Writeable version and then just send the raw bytes.
 * 
 * The original version would do two layers of serialization. This seems to be
 * wasted effort when we can simplify it into one, especially with the more
 * complicated serialization/deserialization.)
 * 
 * Before: (POJO->ThriftFlumeEvent->thrift-> ThriftFlumeEvent -> POJO)
 * 
 * Now: (POJO -> bytes -> thrift -> bytes -> POJO).
 */
public class ThriftRawEventSink extends ThriftEventSink {

  public ThriftRawEventSink(String host, int port) {
    super(host, port);
  }

  @Override
  public void append(Event e) throws IOException {
    WriteableEvent we = new WriteableEvent(e);
    RawEvent re = new RawEvent(we.toBytes());

    try {
      client.rawAppend(re);
      updateAppendStats(e);
    } catch (TException e1) {
      e1.printStackTrace();
      throw new IOException("Append failed " + e);
    }
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... args) {
        if (args.length > 2) {
          throw new IllegalArgumentException(
              "usage: trawsink([hostname, [portno]]) ");
        }
        String host = FlumeConfiguration.get().getCollectorHost();
        int port = FlumeConfiguration.get().getCollectorPort();
        if (args.length >= 1) {
          host = args[0];
        }

        if (args.length >= 2) {
          port = Integer.parseInt(args[1]);
        }

        return new ThriftRawEventSink(host, port);
      }
    };
  }

  public static void main(String argv[]) {
    FlumeConfiguration conf = FlumeConfiguration.get();
    ThriftRawEventSink sink =
        new ThriftRawEventSink("localhost", conf.getCollectorPort());
    try {
      sink.open();

      for (int i = 0; i < 100; i++) {
        Event e = new EventImpl(("This is a test " + i).getBytes());
        sink.append(e);
        Thread.sleep(200);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

  }
}
