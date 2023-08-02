// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.flume.sink.kudu;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.client.KuduClient;

class KuduSinkTestUtil {
  private static final Logger LOG = LoggerFactory.getLogger(KuduSinkTestUtil.class);

  static KuduSink createSink(KuduClient client, String tableName, Context ctx) {
    return createSink(tableName, client, ctx, client.getMasterAddressesAsString());
  }

  private static KuduSink createSink(
      String tableName, KuduClient client, Context ctx, String masterAddresses) {
    LOG.info("Creating Kudu sink for '{}' table...", tableName);

    Context context = new Context();
    context.put(KuduSinkConfigurationConstants.TABLE_NAME, tableName);
    context.put(KuduSinkConfigurationConstants.MASTER_ADDRESSES, masterAddresses);
    context.putAll(ctx.getParameters());
    KuduSink sink = new KuduSink(client);
    Configurables.configure(sink, context);
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    sink.setChannel(channel);

    LOG.info("Created Kudu sink for '{}' table.", tableName);

    return sink;
  }

  static KuduSink createSecureSink(String tableName, String masterAddresses, String clusterRoot) {
    Context context = new Context();
    context.put(KuduSinkConfigurationConstants.KERBEROS_KEYTAB, clusterRoot +
        "/krb5kdc/test-user.keytab");
    context.put(KuduSinkConfigurationConstants.KERBEROS_PRINCIPAL, "test-user@KRBTEST.COM");

    return createSink(tableName, null, context, masterAddresses);
  }

  static void processEventsCreatingSink(
      KuduClient syncClient, Context context, String tableName, List<Event> events)
      throws EventDeliveryException {
    KuduSink sink = createSink(syncClient, tableName, context);
    sink.start();
    processEvents(sink, events);
  }

  static void processEvents(KuduSink sink, List<Event> events) throws EventDeliveryException {
    Channel channel = sink.getChannel();
    Transaction tx = channel.getTransaction();
    tx.begin();
    for (Event e : events) {
      channel.put(e);
    }
    tx.commit();
    tx.close();

    Status status = sink.process();
    if (events.isEmpty()) {
      assertSame("incorrect status for empty channel", status, Status.BACKOFF);
    } else {
      assertNotSame("incorrect status for non-empty channel", status, Status.BACKOFF);
    }
  }
}
