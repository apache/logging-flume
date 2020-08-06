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

import java.lang.reflect.InvocationTargetException;
import java.security.PrivilegedAction;
import java.util.List;

import com.google.common.base.Preconditions;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.auth.FlumeAuthenticationUtil;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.OperationResponse;
import org.apache.kudu.client.SessionConfiguration;

/**
 * A Flume sink that reads events from a channel and writes them to a Kudu table.
 *
 * <p><strong>Flume Kudu Sink configuration parameters</strong>
 *
 * <table cellpadding=3 cellspacing=0 border=1 summary="Flume Kudu Sink configuration parameters">
 * <tr><th>Property Name</th><th>Default</th><th>Required?</th><th>Description</th></tr>
 * <tr><td>channel</td><td></td><td>Yes</td><td>The name of the Flume channel to read.</td></tr>
 * <tr><td>type</td><td></td><td>Yes</td>
 *     <td>Component name. Must be {@code org.apache.kudu.flume.sink.KuduSink}</td></tr>
 * <tr><td>masterAddresses</td><td></td><td>Yes</td>
 *     <td>Comma-separated list of "host:port" Kudu master addresses.
 *     The port is optional.</td></tr>
 * <tr><td>tableName</td><td></td><td>Yes</td>
 *     <td>The name of the Kudu table to write to.</td></tr>
 * <tr><td>batchSize</td><td>1000</td><td>No</td>
 * <td>The maximum number of events the sink takes from the channel per transaction.</td></tr>
 * <tr><td>ignoreDuplicateRows</td><td>true</td>
 *     <td>No</td><td>Whether to ignore duplicate primary key errors caused by inserts.</td></tr>
 * <tr><td>timeoutMillis</td><td>10000</td><td>No</td>
 *     <td>Timeout period for Kudu write operations, in milliseconds.</td></tr>
 * <tr><td>producer</td><td>{@link SimpleKuduOperationsProducer}</td><td>No</td>
 *     <td>The fully-qualified class name of the {@link KuduOperationsProducer}
 *     the sink should use.</td></tr>
 * <tr><td>producer.*</td><td></td><td>(Varies by operations producer)</td>
 *     <td>Configuration properties to pass to the operations producer implementation.</td></tr>
 * </table>
 *
 * <p><strong>Installation</strong>
 *
 * <p>After building the sink, in order to use it with Flume, place the file named
 * <tt>kudu-flume-sink-<em>VERSION</em>-jar-with-dependencies.jar</tt> in the
 * Flume <tt>plugins.d</tt> directory under <tt>kudu-flume-sink/lib/</tt>.
 *
 * <p>For detailed instructions on using Flume's plugins.d mechanism, please see the plugins.d
 * section of the <a href="https://flume.apache.org/FlumeUserGuide.html#the-plugins-d-directory">
 *   Flume User Guide</a>.
 */
public class KuduSink extends AbstractSink implements Configurable {
  private static final Logger logger = LoggerFactory.getLogger(KuduSink.class);
  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final Long DEFAULT_TIMEOUT_MILLIS =
          AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS;
  private static final String DEFAULT_KUDU_OPERATION_PRODUCER =
          "org.apache.flume.sink.kudu.SimpleKuduOperationsProducer";
  private static final boolean DEFAULT_IGNORE_DUPLICATE_ROWS = true;

  private String masterAddresses;
  private String tableName;
  private int batchSize;
  private long timeoutMillis;
  private boolean ignoreDuplicateRows;
  private KuduTable table;
  private KuduSession session;
  private KuduClient client;
  private KuduOperationsProducer operationsProducer;
  private SinkCounter sinkCounter;
  private PrivilegedExecutor privilegedExecutor;

  public KuduSink() {
    this(null);
  }

  public KuduSink(KuduClient kuduClient) {
    this.client = kuduClient;
  }

  @Override
  public synchronized void start() {
    Preconditions.checkState(table == null && session == null,
        "Please call stop before calling start on an old instance.");

    // Client is not null only inside tests.
    if (client == null) {
      // Creating client with FlumeAuthenticator.
      client = privilegedExecutor.execute(
        new PrivilegedAction<KuduClient>() {
          @Override
          public KuduClient run() {
            return new KuduClient.KuduClientBuilder(masterAddresses).build();
          }
        }
      );
    }
    session = client.newSession();
    session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
    session.setTimeoutMillis(timeoutMillis);
    session.setIgnoreAllDuplicateRows(ignoreDuplicateRows);
    session.setMutationBufferSpace(batchSize);

    try {
      table = client.openTable(tableName);
    } catch (Exception ex) {
      sinkCounter.incrementConnectionFailedCount();
      String msg = String.format("Could not open Kudu table '%s'", tableName);
      logger.error(msg, ex);
      throw new FlumeException(msg, ex);
    }
    operationsProducer.initialize(table);

    super.start();
    sinkCounter.incrementConnectionCreatedCount();
    sinkCounter.start();
  }

  @Override
  public synchronized void stop() {
    Exception ex = null;
    try {
      operationsProducer.close();
    } catch (Exception e) {
      ex = e;
      logger.error("Error closing operations producer", e);
    }
    try {
      if (client != null) {
        client.shutdown();
      }
    } catch (Exception e) {
      ex = e;
      logger.error("Error closing client", e);
    }
    client = null;
    table = null;
    session = null;

    sinkCounter.incrementConnectionClosedCount();
    sinkCounter.stop();
    if (ex != null) {
      throw new FlumeException("Error stopping sink", ex);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized void configure(Context context) {
    masterAddresses = context.getString(KuduSinkConfigurationConstants.MASTER_ADDRESSES);
    Preconditions.checkNotNull(masterAddresses,
        "Missing master addresses. Please specify property '%s'.",
        KuduSinkConfigurationConstants.MASTER_ADDRESSES);

    tableName = context.getString(KuduSinkConfigurationConstants.TABLE_NAME);
    Preconditions.checkNotNull(tableName,
        "Missing table name. Please specify property '%s'",
        KuduSinkConfigurationConstants.TABLE_NAME);

    batchSize = context.getInteger(KuduSinkConfigurationConstants.BATCH_SIZE, DEFAULT_BATCH_SIZE);
    timeoutMillis = context.getLong(KuduSinkConfigurationConstants.TIMEOUT_MILLIS,
                                    DEFAULT_TIMEOUT_MILLIS);
    ignoreDuplicateRows = context.getBoolean(KuduSinkConfigurationConstants.IGNORE_DUPLICATE_ROWS,
                                             DEFAULT_IGNORE_DUPLICATE_ROWS);
    String operationProducerType = context.getString(KuduSinkConfigurationConstants.PRODUCER);
    String kerberosPrincipal =
        context.getString(KuduSinkConfigurationConstants.KERBEROS_PRINCIPAL);
    String kerberosKeytab = context.getString(KuduSinkConfigurationConstants.KERBEROS_KEYTAB);
    String proxyUser = context.getString(KuduSinkConfigurationConstants.PROXY_USER);

    privilegedExecutor = FlumeAuthenticationUtil.getAuthenticator(
        kerberosPrincipal, kerberosKeytab).proxyAs(proxyUser);

    // Check for operations producer, if null set default operations producer type.
    if (operationProducerType == null || operationProducerType.isEmpty()) {
      operationProducerType = DEFAULT_KUDU_OPERATION_PRODUCER;
      logger.warn("No Kudu operations producer provided, using default");
    }

    Context producerContext = new Context();
    producerContext.putAll(context.getSubProperties(
        KuduSinkConfigurationConstants.PRODUCER_PREFIX));

    try {
      Class<? extends KuduOperationsProducer> clazz =
          (Class<? extends KuduOperationsProducer>)
          Class.forName(operationProducerType);
      operationsProducer = clazz.getDeclaredConstructor().newInstance();
      operationsProducer.configure(producerContext);
    } catch (ClassNotFoundException | NoSuchMethodException |
        InstantiationException | IllegalAccessException | InvocationTargetException e) {
      logger.error("Could not instantiate Kudu operations producer" , e);
      throw new RuntimeException(e);
    }
    sinkCounter = new SinkCounter(this.getName());
  }

  public synchronized KuduClient getClient() {
    return client;
  }

  @Override
  public synchronized Status process() throws EventDeliveryException {
    if (session.hasPendingOperations()) {
      // If for whatever reason we have pending operations, refuse to process
      // more and tell the caller to try again a bit later. We don't want to
      // pile on the KuduSession.
      return Status.BACKOFF;
    }

    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();

    txn.begin();

    try {
      long txnEventCount = 0;
      for (; txnEventCount < batchSize; txnEventCount++) {
        Event event = channel.take();
        if (event == null) {
          break;
        }

        List<Operation> operations = operationsProducer.getOperations(event);
        for (Operation o : operations) {
          session.apply(o);
        }
      }

      logger.debug("Flushing {} events", txnEventCount);
      List<OperationResponse> responses = session.flush();
      if (responses != null) {
        for (OperationResponse response : responses) {
          // Throw an EventDeliveryException if at least one of the responses was
          // a row error. Row errors can occur for example when an event is inserted
          // into Kudu successfully but the Flume transaction is rolled back for some reason,
          // and a subsequent replay of the same Flume transaction leads to a
          // duplicate key error since the row already exists in Kudu.
          // Note: Duplicate keys will not be reported as errors if ignoreDuplicateRows
          // is enabled in the config.
          if (response.hasRowError()) {
            throw new EventDeliveryException("Failed to flush one or more changes. " +
                "Transaction rolled back: " + response.getRowError().toString());
          }
        }
      }

      if (txnEventCount == 0) {
        sinkCounter.incrementBatchEmptyCount();
      } else if (txnEventCount == batchSize) {
        sinkCounter.incrementBatchCompleteCount();
      } else {
        sinkCounter.incrementBatchUnderflowCount();
      }

      txn.commit();

      if (txnEventCount == 0) {
        return Status.BACKOFF;
      }

      sinkCounter.addToEventDrainSuccessCount(txnEventCount);
      return Status.READY;

    } catch (Throwable e) {
      txn.rollback();

      String msg = "Failed to commit transaction. Transaction rolled back.";
      logger.error(msg, e);
      if (e instanceof Error || e instanceof RuntimeException) {
        throw new RuntimeException(e);
      } else {
        logger.error(msg, e);
        throw new EventDeliveryException(msg, e);
      }
    } finally {
      txn.close();
    }
  }
}
