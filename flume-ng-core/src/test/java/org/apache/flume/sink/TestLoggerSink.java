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

package org.apache.flume.sink;

import com.google.common.base.Strings;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleException;
import org.junit.Before;
import org.junit.Test;

public class TestLoggerSink {

  private LoggerSink sink;

  @Before
  public void setUp() {
    sink = new LoggerSink();
  }

  /**
   * Lack of exception test.
   */
  @Test
  public void testAppend() throws InterruptedException, LifecycleException,
      EventDeliveryException {

    Channel channel = new PseudoTxnMemoryChannel();
    Context context = new Context();
    Configurables.configure(channel, context);
    Configurables.configure(sink, context);

    sink.setChannel(channel);
    sink.start();

    for (int i = 0; i < 10; i++) {
      Event event = EventBuilder.withBody(("Test " + i).getBytes());
      channel.put(event);
      sink.process();
    }

    sink.stop();
  }

  @Test
  public void testAppendWithCustomSize() throws InterruptedException, LifecycleException,
          EventDeliveryException {

    Channel channel = new PseudoTxnMemoryChannel();
    Context context = new Context();
    context.put(LoggerSink.MAX_BYTES_DUMP_KEY, String.valueOf(30));
    Configurables.configure(channel, context);
    Configurables.configure(sink, context);

    sink.setChannel(channel);
    sink.start();

    for (int i = 0; i < 10; i++) {
      Event event = EventBuilder.withBody((Strings.padStart("Test " + i, 30, 'P')).getBytes());

      channel.put(event);
      sink.process();
    }

    sink.stop();
  }

}
