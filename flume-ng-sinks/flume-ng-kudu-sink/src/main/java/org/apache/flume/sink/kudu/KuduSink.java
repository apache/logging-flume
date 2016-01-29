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
package org.apache.flume.sink.kudu;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.kududb.client.AsyncKuduClient;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;
import org.kududb.client.Operation;
import org.kududb.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 *
 * A simple sink which reads events from a channel and writes them to Kudu.
 * To use this sink, it has to be configured with certain mandatory parameters:<p>
 * <tt>table: </tt> The name of the table in Hbase to write to. <p>
 */
public class KuduSink extends AbstractSink implements Configurable {
  private static final Logger logger = LoggerFactory.getLogger(KuduSink.class);
  private static final Long DEFAULT_BATCH_SIZE = 100L;
  private static final Long DEFAULT_TIMEOUT_MILLIS = AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS;

  private String masterHost;
  private String tableName;
  private long batchSize;
  private long timeoutMillis;
  private KuduTable table;
  private KuduSession session;
  private KuduClient client;
  private KuduEventSerializer serializer;
  private String eventSerializerType;
  private Context serializerContext;
  private SinkCounter sinkCounter;
  private final CounterGroup counterGroup = new CounterGroup();

  public KuduSink(){
    this(null, null);
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  public KuduSink(String masterHost, String tableName){
    this.masterHost = masterHost;
    this.tableName = tableName;
  }

  @Override
  public void start(){
    Preconditions.checkArgument(table == null || client == null || session == null, "Please call stop " +
        "before calling start on an old instance.");

    client = new KuduClient.KuduClientBuilder(masterHost).build();
    session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    session.setTimeoutMillis(timeoutMillis);

    try {
      table = client.openTable(tableName);
    } catch (Exception e) {
      sinkCounter.incrementConnectionFailedCount();
      logger.error("Could not open table, " + tableName +
          " from HBase", e);
      throw new FlumeException("Could not open table, " + tableName +
          " from HBase", e);
    }

    super.start();
    sinkCounter.incrementConnectionCreatedCount();
    sinkCounter.start();
  }

  @Override
  public void stop(){
    try {
      if (client != null) {
        client.shutdown();
      }
      client = null;
      table = null;
      session = null;
    } catch (Exception e) {
      throw new FlumeException("Error closing client.", e);
    }
    sinkCounter.incrementConnectionClosedCount();
    sinkCounter.stop();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void configure(Context context){
    masterHost = context.getString(KuduSinkConfigurationConstants.CONFIG_MASTER_HOST);
    tableName = context.getString(KuduSinkConfigurationConstants.CONFIG_TABLE);
    batchSize = context.getLong(
            KuduSinkConfigurationConstants.CONFIG_BATCH_SIZE, DEFAULT_BATCH_SIZE);
    timeoutMillis = context.getLong(
            KuduSinkConfigurationConstants.CONFIG_TIMEOUT_MILLIS, DEFAULT_TIMEOUT_MILLIS);
    serializerContext = new Context();
    //If not specified, will use HBase defaults.
    eventSerializerType = context.getString(
            KuduSinkConfigurationConstants.CONFIG_SERIALIZER);
    Preconditions.checkNotNull(masterHost,
        "Master name cannot be empty, please specify in configuration file");
    Preconditions.checkNotNull(tableName,
        "Table name cannot be empty, please specify in configuration file");
    //Check foe event serializer, if null set event serializer type
    if(eventSerializerType == null || eventSerializerType.isEmpty()) {
      eventSerializerType =
          "org.apache.flume.sink.kudu.SimpleKuduEventSerializer";
      logger.info("No serializer defined, Will use default");
    }
    serializerContext.putAll(context.getSubProperties(
            KuduSinkConfigurationConstants.CONFIG_SERIALIZER_PREFIX));
    try {
      Class<? extends KuduEventSerializer> clazz =
          (Class<? extends KuduEventSerializer>)
          Class.forName(eventSerializerType);
      serializer = clazz.newInstance();
      serializer.configure(serializerContext);
    } catch (Exception e) {
      logger.error("Could not instantiate event serializer." , e);
      Throwables.propagate(e);
    }
    sinkCounter = new SinkCounter(this.getName());
  }

  public KuduClient getClient() {
    return client;
  }

  @Override
  public Status process() throws EventDeliveryException {
    if (session.hasPendingOperations()) {
      //if for whatever reason we have pending operations then just refuse to process
      //and tell caller to try again a bit later. We don't want to pile on the kudu
      //session object.
      return Status.READY;
    }

    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();
    long i = 0;

    try {
      txn.begin();

      for (; i < batchSize; i++) {
        Event event = channel.take();
        if (event == null) {
          if (i == 0) {
            status = Status.BACKOFF;
            sinkCounter.incrementBatchEmptyCount();
          } else {
            sinkCounter.incrementBatchUnderflowCount();
          }
          break;
        } else {
          sinkCounter.incrementBatchCompleteCount();
          sinkCounter.addToEventDrainAttemptCount(1);
          serializer.initialize(event, table);
          List<Operation> operations = serializer.getOperations();
          for (Operation o : operations) {
            session.apply(o);
          }
        }
      }

      try {
        logger.debug("Flushing {} events", i);
        session.flush();
      } catch (Exception e) {
        throw new EventDeliveryException("Failed to flush changes." +
                "Transaction rolled back.", e);
      }

      txn.commit();
      sinkCounter.addToEventDrainSuccessCount(i);

    } catch (Throwable e) {
      try{
        txn.rollback();
      } catch (Exception e2) {
        logger.error("Exception in rollback. Rollback might not have been " +
            "successful." , e2);
      }
      logger.error("Failed to commit transaction." +
          "Transaction rolled back.", e);
      if(e instanceof Error || e instanceof RuntimeException){
        logger.error("Failed to commit transaction." +
            "Transaction rolled back.", e);
        Throwables.propagate(e);
      } else {
        logger.error("Failed to commit transaction." +
            "Transaction rolled back.", e);
        throw new EventDeliveryException("Failed to commit transaction." +
            "Transaction rolled back.", e);
      }
    } finally {
      txn.close();
    }
    return status;
  }

  @VisibleForTesting
  @InterfaceAudience.Private
  KuduEventSerializer getSerializer() {
    return serializer;
  }
}
