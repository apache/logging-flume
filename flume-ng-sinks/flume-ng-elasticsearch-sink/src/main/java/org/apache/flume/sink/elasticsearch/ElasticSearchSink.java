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
package org.apache.flume.sink.elasticsearch;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.BATCH_SIZE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.CLUSTER_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_CLUSTER_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_INDEX_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_INDEX_TYPE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_PORT;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_TTL;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.HOSTNAMES;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_TYPE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.SERIALIZER;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.SERIALIZER_PREFIX;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.TTL;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * A sink which reads events from a channel and writes them to ElasticSearch
 * based on the work done by https://github.com/Aconex/elasticflume.git.</p>
 *
 * This sink supports batch reading of events from the channel and writing them
 * to ElasticSearch.</p>
 *
 * Indexes will be rolled daily using the format 'indexname-YYYY-MM-dd' to allow
 * easier management of the index</p>
 *
 * This sink must be configured with with mandatory parameters detailed in
 * {@link ElasticSearchSinkConstants}</p>
 * It is recommended as a secondary step the ElasticSearch indexes are optimized
 * for the specified serializer. This is not handled by the sink but is
 * typically done by deploying a config template alongside the ElasticSearch
 * deploy</p>
 * @see http
 *      ://www.elasticsearch.org/guide/reference/api/admin-indices-templates.
 *      html
 */
public class ElasticSearchSink extends AbstractSink implements Configurable {

  private static final Logger logger = LoggerFactory
      .getLogger(ElasticSearchSink.class);

  // Used for testing
  private boolean isLocal = false;
  private final CounterGroup counterGroup = new CounterGroup();

  private static final int defaultBatchSize = 100;

  private int batchSize = defaultBatchSize;
  private long ttlMs = DEFAULT_TTL;
  private String clusterName = DEFAULT_CLUSTER_NAME;
  private String indexName = DEFAULT_INDEX_NAME;
  private String indexType = DEFAULT_INDEX_TYPE;

  private InetSocketTransportAddress[] serverAddresses;

  private Node node;
  private Client client;
  private ElasticSearchIndexRequestBuilderFactory indexRequestFactory;
  private SinkCounter sinkCounter;

  /**
   * Create an {@link ElasticSearchSink} configured using the supplied
   * configuration
   */
  public ElasticSearchSink() {
    this(false);
  }

  /**
   * Create an {@link ElasticSearchSink}</p>
   *
   * @param isLocal
   *          If <tt>true</tt> sink will be configured to only talk to an
   *          ElasticSearch instance hosted in the same JVM, should always be
   *          false is production
   *
   */
  @VisibleForTesting
  ElasticSearchSink(boolean isLocal) {
    this.isLocal = isLocal;
  }

  @VisibleForTesting
  InetSocketTransportAddress[] getServerAddresses() {
    return serverAddresses;
  }

  @VisibleForTesting
  String getClusterName() {
    return clusterName;
  }

  @VisibleForTesting
  String getIndexName() {
    return indexName;
  }

  @VisibleForTesting
  String getIndexType() {
    return indexType;
  }

  @VisibleForTesting
  long getTTLMs() {
    return ttlMs;
  }

  @Override
  public Status process() throws EventDeliveryException {
    logger.debug("processing...");
    Status status = Status.READY;
    Channel channel = getChannel();
    Transaction txn = channel.getTransaction();
    try {
      txn.begin();
      BulkRequestBuilder bulkRequest = client.prepareBulk();
      for (int i = 0; i < batchSize; i++) {
        Event event = channel.take();

        if (event == null) {
          break;
        }

        IndexRequestBuilder indexRequest =
            indexRequestFactory.createIndexRequest(
                client, indexName, indexType, event);

        if (ttlMs > 0) {
          indexRequest.setTTL(ttlMs);
        }

        bulkRequest.add(indexRequest);
      }

      int size = bulkRequest.numberOfActions();
      if (size <= 0) {
        sinkCounter.incrementBatchEmptyCount();
        counterGroup.incrementAndGet("channel.underflow");
        status = Status.BACKOFF;
      } else {
        if (size < batchSize) {
          sinkCounter.incrementBatchUnderflowCount();
          status = Status.BACKOFF;
        } else {
          sinkCounter.incrementBatchCompleteCount();
        }

        sinkCounter.addToEventDrainAttemptCount(size);

        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
          throw new EventDeliveryException(bulkResponse.buildFailureMessage());
        }
      }
      txn.commit();
      sinkCounter.addToEventDrainSuccessCount(size);
      counterGroup.incrementAndGet("transaction.success");
    } catch (Throwable ex) {
      try {
        txn.rollback();
        counterGroup.incrementAndGet("transaction.rollback");
      } catch (Exception ex2) {
        logger.error(
            "Exception in rollback. Rollback might not have been successful.",
            ex2);
      }

      if (ex instanceof Error || ex instanceof RuntimeException) {
        logger.error("Failed to commit transaction. Transaction rolled back.",
            ex);
        Throwables.propagate(ex);
      } else {
        logger.error("Failed to commit transaction. Transaction rolled back.",
            ex);
        throw new EventDeliveryException(
            "Failed to commit transaction. Transaction rolled back.", ex);
      }
    } finally {
      txn.close();
    }
    return status;
  }

  @Override
  public void configure(Context context) {
    if (!isLocal) {
      String[] hostNames = null;
      if (StringUtils.isNotBlank(context.getString(HOSTNAMES))) {
        hostNames = context.getString(HOSTNAMES).split(",");
      }
      Preconditions.checkState(hostNames != null && hostNames.length > 0,
          "Missing Param:" + HOSTNAMES);

      serverAddresses = new InetSocketTransportAddress[hostNames.length];
      for (int i = 0; i < hostNames.length; i++) {
        String[] hostPort = hostNames[i].split(":");
        String host = hostPort[0];
        int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1])
            : DEFAULT_PORT;
        serverAddresses[i] = new InetSocketTransportAddress(host, port);
      }

      Preconditions.checkState(serverAddresses != null
          && serverAddresses.length > 0, "Missing Param:" + HOSTNAMES);
    }

    if (StringUtils.isNotBlank(context.getString(INDEX_NAME))) {
      this.indexName = context.getString(INDEX_NAME);
    }

    if (StringUtils.isNotBlank(context.getString(INDEX_TYPE))) {
      this.indexType = context.getString(INDEX_TYPE);
    }

    if (StringUtils.isNotBlank(context.getString(CLUSTER_NAME))) {
      this.clusterName = context.getString(CLUSTER_NAME);
    }

    if (StringUtils.isNotBlank(context.getString(BATCH_SIZE))) {
      this.batchSize = Integer.parseInt(context.getString(BATCH_SIZE));
    }

    if (StringUtils.isNotBlank(context.getString(TTL))) {
      this.ttlMs = TimeUnit.DAYS.toMillis(Integer.parseInt(context
          .getString(TTL)));
      Preconditions.checkState(ttlMs > 0, TTL
          + " must be greater than 0 or not set.");
    }

    String serializerClazz = "org.apache.flume.sink.elasticsearch.ElasticSearchLogStashEventSerializer";
    if (StringUtils.isNotBlank(context.getString(SERIALIZER))) {
      serializerClazz = context.getString(SERIALIZER);
    }

    Context serializerContext = new Context();
    serializerContext.putAll(context.getSubProperties(SERIALIZER_PREFIX));

    try {
      @SuppressWarnings("unchecked")
      Class<? extends Configurable> clazz = (Class<? extends Configurable>) Class
          .forName(serializerClazz);
      Configurable serializer = clazz.newInstance();
      if (serializer instanceof ElasticSearchIndexRequestBuilderFactory) {
        indexRequestFactory = (ElasticSearchIndexRequestBuilderFactory) serializer;
      } else if (serializer instanceof ElasticSearchEventSerializer){
        indexRequestFactory = new EventSerializerIndexRequestBuilderFactory(
            (ElasticSearchEventSerializer) serializer);
      } else {
          throw new IllegalArgumentException(
              serializerClazz + " is neither an ElasticSearchEventSerializer"
              + " nor an ElasticSearchIndexRequestBuilderFactory.");
      }
      indexRequestFactory.configure(serializerContext);
    } catch (Exception e) {
      logger.error("Could not instantiate event serializer.", e);
      Throwables.propagate(e);
    }

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }

    Preconditions.checkState(StringUtils.isNotBlank(indexName),
        "Missing Param:" + INDEX_NAME);
    Preconditions.checkState(StringUtils.isNotBlank(indexType),
        "Missing Param:" + INDEX_TYPE);
    Preconditions.checkState(StringUtils.isNotBlank(clusterName),
        "Missing Param:" + CLUSTER_NAME);
    Preconditions.checkState(batchSize >= 1, BATCH_SIZE
        + " must be greater than 0");
  }

  @Override
  public void start() {
    logger.info("ElasticSearch sink {} started");
    sinkCounter.start();
    try {
      openConnection();
    } catch (Exception ex) {
      sinkCounter.incrementConnectionFailedCount();
      closeConnection();
    }

    super.start();
  }

  @Override
  public void stop() {
    logger.info("ElasticSearch sink {} stopping");
    closeConnection();

    sinkCounter.stop();
    super.stop();
  }

  private void openConnection() {
    if (isLocal) {
      logger.info("Using ElasticSearch AutoDiscovery mode");
      openLocalDiscoveryClient();
    } else {
      logger.info("Using ElasticSearch hostnames: {} ",
          Arrays.toString(serverAddresses));
      openClient();
    }
    sinkCounter.incrementConnectionCreatedCount();
  }

  /*
   * FOR TESTING ONLY...
   *
   * Opens a local discovery node for talking to an elasticsearch server running
   * in the same JVM
   */
  private void openLocalDiscoveryClient() {
    node = NodeBuilder.nodeBuilder().client(true).local(true).node();
    client = node.client();
  }

  private void openClient() {
    Settings settings = ImmutableSettings.settingsBuilder()
        .put("cluster.name", clusterName).build();

    TransportClient transport = new TransportClient(settings);
    for (InetSocketTransportAddress host : serverAddresses) {
      transport.addTransportAddress(host);
    }
    client = transport;
  }

  private void closeConnection() {
    if (client != null) {
      client.close();
    }
    client = null;

    if (node != null) {
      node.close();
    }
    node = null;

    sinkCounter.incrementConnectionClosedCount();
  }
}
