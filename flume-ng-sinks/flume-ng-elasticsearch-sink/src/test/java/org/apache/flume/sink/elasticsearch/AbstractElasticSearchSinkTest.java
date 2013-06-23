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
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_TYPE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.TTL;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.Gateway;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.node.internal.InternalNode;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.joda.time.DateTimeUtils;
import org.junit.After;
import org.junit.Before;

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
    parameters = Maps.newHashMap();
    parameters.put(INDEX_NAME, DEFAULT_INDEX_NAME);
    parameters.put(INDEX_TYPE, DEFAULT_INDEX_TYPE);
    parameters.put(CLUSTER_NAME, DEFAULT_CLUSTER_NAME);
    parameters.put(BATCH_SIZE, "1");
    parameters.put(TTL, "5");

    timestampedIndexName = DEFAULT_INDEX_NAME + '-'
        + ElasticSearchIndexRequestBuilderFactory.df.format(FIXED_TIME_MILLIS);
  }

  void createNodes() throws Exception {
    Settings settings = ImmutableSettings
        .settingsBuilder()
        .put("number_of_shards", 1)
        .put("number_of_replicas", 0)
        .put("routing.hash.type", "simple")
        .put("gateway.type", "none")
        .put("path.data", "target/es-test")
        .build();

    node = NodeBuilder.nodeBuilder().settings(settings).local(true).node();
    client = node.client();

    client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute()
        .actionGet();
  }

  void shutdownNodes() throws Exception {
    ((InternalNode) node).injector().getInstance(Gateway.class).reset();
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
        events);
  }

  void assertBodyQuery(int expectedHits, Event... events) {
    // Perform Multi Field Match
    assertSearch(expectedHits,
        performSearch(QueryBuilders.fieldQuery("@message", "event")));
  }

  SearchResponse performSearch(QueryBuilder query) {
    return client.prepareSearch(timestampedIndexName)
        .setTypes(DEFAULT_INDEX_TYPE).setQuery(query).execute().actionGet();
  }

  void assertSearch(int expectedHits, SearchResponse response, Event... events) {
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
      Map<String, Object> source = hit.getSource();
      assertEquals(new String(event.getBody()), source.get("@message"));
    }
  }
}
