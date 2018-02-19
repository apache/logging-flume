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

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.sink.elasticsearch.client.ElasticSearchTransportClient;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.Netty4Plugin;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.UUID;

import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.BATCH_SIZE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.CLUSTER_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_TYPE;
import static org.junit.Assert.assertEquals;

public abstract class AbstractElasticSearchSinkTest {

  static final String DEFAULT_INDEX_NAME = "flume";
  static final String DEFAULT_INDEX_TYPE = "log";
  static final String DEFAULT_CLUSTER_NAME = "elasticsearch";
  static final long FIXED_TIME_MILLIS = 123456789L;

  Node node;
  Client client;
  String timestampedIndexName;
  Map<String, String> parameters;

  void initDefaults() {
    parameters = MapBuilder.<String, String>newMapBuilder().map();
    parameters.put(INDEX_NAME, DEFAULT_INDEX_NAME);
    parameters.put(INDEX_TYPE, DEFAULT_INDEX_TYPE);
    parameters.put(CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
    parameters.put(BATCH_SIZE, "1");

    timestampedIndexName = DEFAULT_INDEX_NAME + '-'
        + ElasticSearchIndexRequestBuilderFactory.df.format(FIXED_TIME_MILLIS);
  }

  void createNodes() throws Exception {
    String hostname = "localhost";
    Settings settings = Settings.builder()
        .put("cluster.name", "test")
        // required to build a cluster, replica tests will test this.
        .put("discovery.zen.ping.unicast.hosts", hostname)
        .put("transport.type", Netty4Plugin.NETTY_TRANSPORT_NAME)
        .put("network.host", hostname)
        .put("http.enabled", false)
        .put("path.home", "target/es-test" + UUID.randomUUID())
        // maximum five nodes on same host
        .put("node.max_local_storage_nodes", 5)
        .put("thread_pool.bulk.size", Runtime.getRuntime().availableProcessors())
        // default is 50 which is too low
        .put("thread_pool.bulk.queue_size", 16 * Runtime.getRuntime().availableProcessors())
        .build();

    node = new MockNode(settings, Netty4Plugin.class);
    node.start();
    client = node.client();

    ElasticSearchTransportClient.testClient = (AbstractClient) client;
    client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute()
        .actionGet();
  }

  void shutdownNodes() throws Exception {
    client.close();
    node.close();
  }

  @Before
  public void setFixedJodaTime() {
    DateTimeUtils.setCurrentMillisFixed(FIXED_TIME_MILLIS);
  }

  @After
  public void resetJodaTime() {
    DateTimeUtils.setCurrentMillisSystem();
  }

  Channel bindAndStartChannel(ElasticSearchSink fixture) {
    // Configure the channel
    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());

    // Wire them together
    fixture.setChannel(channel);
    fixture.start();
    return channel;
  }

  void assertMatchAllQuery(int expectedHits, Event... events) {
    assertSearch(expectedHits, performSearch(QueryBuilders.matchAllQuery()),
        null, events);
  }

  void assertBodyQuery(int expectedHits, Event... events) {
    // Perform Multi Field Match
    assertSearch(expectedHits,
        performSearch(QueryBuilders.matchQuery("@message", "event")),
        null, events);
  }

  SearchResponse performSearch(QueryBuilder query) {
    return client.prepareSearch(timestampedIndexName)
        .setTypes(DEFAULT_INDEX_TYPE).setQuery(query).execute().actionGet();
  }

  void assertSearch(int expectedHits, SearchResponse response, Map<String, Object> expectedBody,
                    Event... events) {
    SearchHits hitResponse = response.getHits();
    assertEquals(expectedHits, hitResponse.getTotalHits());

    SearchHit[] hits = hitResponse.getHits();
    Arrays.sort(hits, new Comparator<SearchHit>() {
      @Override
      public int compare(SearchHit o1, SearchHit o2) {
        return o1.getSourceAsString().compareTo(o2.getSourceAsString());
      }
    });

    for (int i = 0; i < events.length; i++) {
      Event event = events[i];
      SearchHit hit = hits[i];
      Map<String, Object> source = hit.getSourceAsMap();
      if (expectedBody == null) {
        assertEquals(new String(event.getBody()), source.get("@message"));
      } else {
        assertEquals(expectedBody, source.get("@message"));
      }
    }
  }

}
