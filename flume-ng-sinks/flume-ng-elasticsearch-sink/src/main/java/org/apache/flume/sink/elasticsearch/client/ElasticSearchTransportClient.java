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
package org.apache.flume.sink.elasticsearch.client;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.ElasticSearchIndexRequestBuilderFactory;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.DEFAULT_PORT;

public class ElasticSearchTransportClient implements ElasticSearchClient {

  public static final Logger logger = LoggerFactory
      .getLogger(ElasticSearchTransportClient.class);

  private TransportAddress[] serverAddresses;
  private ElasticSearchEventSerializer serializer;
  private ElasticSearchIndexRequestBuilderFactory indexRequestBuilderFactory;
  private BulkRequestBuilder bulkRequestBuilder;
  private boolean openCompress = false;

  private Client client;

  @VisibleForTesting
  TransportAddress[] getServerAddresses() {
    return serverAddresses;
  }

  @VisibleForTesting
  void setBulkRequestBuilder(BulkRequestBuilder bulkRequestBuilder) {
    this.bulkRequestBuilder = bulkRequestBuilder;
  }

  /**
   * Transport client for external cluster
   * 
   * @param hostNames
   * @param clusterName
   * @param serializer
   */
  public ElasticSearchTransportClient(String[] hostNames, String clusterName, boolean compress,
      ElasticSearchEventSerializer serializer) {
    configureHostnames(hostNames);
    this.serializer = serializer;
    this.openCompress = compress;
    openClient(clusterName);
  }

  public ElasticSearchTransportClient(String[] hostNames, String clusterName, boolean compress,
      ElasticSearchIndexRequestBuilderFactory indexBuilder) {
    configureHostnames(hostNames);
    this.indexRequestBuilderFactory = indexBuilder;
    this.openCompress = compress;
    openClient(clusterName);
  }
  
  /**
   * Local transport client only for testing
   * 
   * @param indexBuilderFactory
   */
  public ElasticSearchTransportClient(ElasticSearchIndexRequestBuilderFactory indexBuilderFactory) {
    this.indexRequestBuilderFactory = indexBuilderFactory;
    openLocalDiscoveryClient();
  }
  
  /**
   * Local transport client only for testing
   *
   * @param serializer
   */
  public ElasticSearchTransportClient(ElasticSearchEventSerializer serializer) {
    this.serializer = serializer;
    openLocalDiscoveryClient();
  }

  /**
   * Used for testing
   *
   * @param client
   *    ElasticSearch Client
   * @param serializer
   *    Event Serializer
   */
  public ElasticSearchTransportClient(Client client,
      ElasticSearchEventSerializer serializer) {
    this.client = client;
    this.serializer = serializer;
  }

  /**
   * Used for testing
   */
  public ElasticSearchTransportClient(Client client,
                                      ElasticSearchIndexRequestBuilderFactory requestBuilderFactory)
      throws IOException {
    this.client = client;
    requestBuilderFactory.createIndexRequest(client, null, null, null);
  }

  private void configureHostnames(String[] hostNames) {
    logger.warn(Arrays.toString(hostNames));
    serverAddresses = new TransportAddress[hostNames.length];
    for (int i = 0; i < hostNames.length; i++) {
      String[] hostPort = hostNames[i].trim().split(":");
      String host = hostPort[0].trim();
      int port = hostPort.length == 2 ? Integer.parseInt(hostPort[1].trim())
              : DEFAULT_PORT;
      serverAddresses[i] = new TransportAddress(new InetSocketAddress(host, port));
    }
  }
  
  @Override
  public void close() {
    if (client != null) {
      client.close();
    }
    client = null;
  }

  @Override
  public void addEvent(Event event, IndexNameBuilder indexNameBuilder,
      String indexType) throws Exception {
    if (bulkRequestBuilder == null) {
      bulkRequestBuilder = client.prepareBulk();
    }

    IndexRequestBuilder indexRequestBuilder = null;
    if (indexRequestBuilderFactory == null) {
      indexRequestBuilder = client
          .prepareIndex(indexNameBuilder.getIndexName(event), indexType)
          .setSource(serializer.getContentBuilder(event).bytes(), XContentType.JSON);
    } else {
      indexRequestBuilder = indexRequestBuilderFactory.createIndexRequest(
          client, indexNameBuilder.getIndexPrefix(event), indexType, event);
    }

    bulkRequestBuilder.add(indexRequestBuilder);
  }

  @Override
  public void execute() throws Exception {
    try {
      BulkResponse bulkResponse = bulkRequestBuilder.execute().actionGet();
      if (bulkResponse.hasFailures()) {
        throw new EventDeliveryException(bulkResponse.buildFailureMessage());
      }
    } finally {
      bulkRequestBuilder = client.prepareBulk();
    }
  }

  /**
   * Open client to elaticsearch cluster
   * 
   * @param clusterName
   */
  private void openClient(String clusterName) {
    logger.info("Using ElasticSearch hostnames: {} ",
        Arrays.toString(serverAddresses));
    Settings.Builder settingsBuilder = Settings.builder()
        .put("cluster.name", clusterName);
    if (openCompress) {
      logger.info("Open compress option for transport client.");
      settingsBuilder.put("transport.tcp.compress", true);
    }

    Settings settings = settingsBuilder.build();
    TransportClient transportClient = new PreBuiltTransportClient(settings);
    for (TransportAddress host : serverAddresses) {
      transportClient.addTransportAddress(host);
    }
    if (client != null) {
      client.close();
    }
    client = transportClient;
  }

  /*
   * FOR TESTING ONLY...
   * 
   * Opens a local discovery node for talking to an elasticsearch server running
   * in the same JVM
   */
  private void openLocalDiscoveryClient() {
    // use local node created in AbstractElasticSearchSinkTest.class
    // which is 127.0.0.1:9200
    Settings settings = Settings.builder()
            .put("cluster.name", "test")
            .build();
    if (client != null) {
      client.close();
    }
    findNodeAddress();
    try {
      client = new PreBuiltTransportClient(settings).addTransportAddress(
              new TransportAddress(InetAddress.getByName(host), port));
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
  }

  private String host;
  private int port;
  // FOR TESTING
  public static AbstractClient testClient;

  private void findNodeAddress() {
    NodesInfoRequest nodesInfoRequest = new NodesInfoRequest().transport(true);
    NodesInfoResponse response = testClient.admin().cluster()
        .nodesInfo(nodesInfoRequest).actionGet();
    TransportAddress address = response.getNodes().iterator().next().getTransport().getAddress()
            .publishAddress();
    host = address.getAddress();
    port = address.getPort();
    if (host == null) {
      throw new IllegalArgumentException("host not found");
    }
  }

  @Override
  public void configure(Context context) {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
