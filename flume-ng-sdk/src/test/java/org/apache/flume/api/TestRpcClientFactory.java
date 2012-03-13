/*
 * Copyright 2012 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.api;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.ipc.Server;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcTestUtils.OKAvroHandler;
import org.junit.Test;

import org.apache.flume.event.EventBuilder;

/**
 * Very light testing on the factory. The heavy testing is done on the test
 * dedicated to the implementation.
 */
public class TestRpcClientFactory {

  private static final String localhost = "localhost";

  @Test
  public void testTwoParamSimpleAppend() throws FlumeException,
      EventDeliveryException {
    RpcClient client = null;
    Server server = RpcTestUtils.startServer(new OKAvroHandler());
    try {
      client = RpcClientFactory.getInstance(localhost, server.getPort());
      client.append(EventBuilder.withBody("wheee!!!", Charset.forName("UTF8")));
    } finally {
      RpcTestUtils.stopServer(server);
      if (client != null) client.close();
    }
  }

  @Test
  public void testThreeParamBatchAppend() throws FlumeException,
      EventDeliveryException {
    int batchSize = 7;
    RpcClient client = null;
    Server server = RpcTestUtils.startServer(new OKAvroHandler());
    try {
      client = RpcClientFactory.getInstance(localhost, server.getPort(),
          batchSize);

      List<Event> events = new ArrayList<Event>();
      for (int i = 0; i < batchSize; i++) {
        events.add(EventBuilder.withBody("evt: " + i, Charset.forName("UTF8")));
      }
      client.appendBatch(events);
    } finally {
      RpcTestUtils.stopServer(server);
      if (client != null) client.close();
    }
  }

  // we are supposed to handle this gracefully
  @Test
  public void testTwoParamBatchAppendOverflow() throws FlumeException,
      EventDeliveryException {
    RpcClient client = null;
    Server server = RpcTestUtils.startServer(new OKAvroHandler());
    try {
      client = RpcClientFactory.getInstance(localhost, server.getPort());
      int batchSize = client.getBatchSize();
      int moreThanBatch = batchSize + 1;
      List<Event> events = new ArrayList<Event>();
      for (int i = 0; i < moreThanBatch; i++) {
        events.add(EventBuilder.withBody("evt: " + i, Charset.forName("UTF8")));
      }
      client.appendBatch(events);
    } finally {
      RpcTestUtils.stopServer(server);
      if (client != null) client.close();
    }
  }

}
