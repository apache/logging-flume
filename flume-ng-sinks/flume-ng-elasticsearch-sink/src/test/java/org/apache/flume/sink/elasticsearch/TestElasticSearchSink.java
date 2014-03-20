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
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.HOSTNAMES;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_NAME;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.INDEX_TYPE;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.SERIALIZER;
import static org.apache.flume.sink.elasticsearch.ElasticSearchSinkConstants.TTL;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.time.FastDateFormat;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.UUID;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.io.FastByteArrayOutputStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestElasticSearchSink extends AbstractElasticSearchSinkTest {

  private ElasticSearchSink fixture;

  @Before
  public void init() throws Exception {
    initDefaults();
    createNodes();
    fixture = new ElasticSearchSink(true);
    fixture.setName("ElasticSearchSink-" + UUID.randomUUID().toString());
  }

  @After
  public void tearDown() throws Exception {
    shutdownNodes();
  }

  @Test
  public void shouldIndexOneEvent() throws Exception {
    Configurables.configure(fixture, new Context(parameters));
    Channel channel = bindAndStartChannel(fixture);

    Transaction tx = channel.getTransaction();
    tx.begin();
    Event event = EventBuilder.withBody("event #1 or 1".getBytes());
    channel.put(event);
    tx.commit();
    tx.close();

    fixture.process();
    fixture.stop();
    client.admin().indices()
        .refresh(Requests.refreshRequest(timestampedIndexName)).actionGet();

    assertMatchAllQuery(1, event);
    assertBodyQuery(1, event);
  }

  @Test
  public void shouldIndexFiveEvents() throws Exception {
    // Make it so we only need to call process once
    parameters.put(BATCH_SIZE, "5");
    Configurables.configure(fixture, new Context(parameters));
    Channel channel = bindAndStartChannel(fixture);

    int numberOfEvents = 5;
    Event[] events = new Event[numberOfEvents];

    Transaction tx = channel.getTransaction();
    tx.begin();
    for (int i = 0; i < numberOfEvents; i++) {
      String body = "event #" + i + " of " + numberOfEvents;
      Event event = EventBuilder.withBody(body.getBytes());
      events[i] = event;
      channel.put(event);
    }
    tx.commit();
    tx.close();

    fixture.process();
    fixture.stop();
    client.admin().indices()
        .refresh(Requests.refreshRequest(timestampedIndexName)).actionGet();

    assertMatchAllQuery(numberOfEvents, events);
    assertBodyQuery(5, events);
  }

  @Test
  public void shouldIndexFiveEventsOverThreeBatches() throws Exception {
    parameters.put(BATCH_SIZE, "2");
    Configurables.configure(fixture, new Context(parameters));
    Channel channel = bindAndStartChannel(fixture);

    int numberOfEvents = 5;
    Event[] events = new Event[numberOfEvents];

    Transaction tx = channel.getTransaction();
    tx.begin();
    for (int i = 0; i < numberOfEvents; i++) {
      String body = "event #" + i + " of " + numberOfEvents;
      Event event = EventBuilder.withBody(body.getBytes());
      events[i] = event;
      channel.put(event);
    }
    tx.commit();
    tx.close();

    int count = 0;
    Status status = Status.READY;
    while (status != Status.BACKOFF) {
      count++;
      status = fixture.process();
    }
    fixture.stop();

    assertEquals(3, count);

    client.admin().indices()
        .refresh(Requests.refreshRequest(timestampedIndexName)).actionGet();
    assertMatchAllQuery(numberOfEvents, events);
    assertBodyQuery(5, events);
  }

  @Test
  public void shouldParseConfiguration() {
    parameters.put(HOSTNAMES, "10.5.5.27");
    parameters.put(CLUSTER_NAME, "testing-cluster-name");
    parameters.put(INDEX_NAME, "testing-index-name");
    parameters.put(INDEX_TYPE, "testing-index-type");
    parameters.put(TTL, "10");

    fixture = new ElasticSearchSink();
    fixture.configure(new Context(parameters));

    String[] expected = { "10.5.5.27" };

    assertEquals("testing-cluster-name", fixture.getClusterName());
    assertEquals("testing-index-name", fixture.getIndexName());
    assertEquals("testing-index-type", fixture.getIndexType());
    assertEquals(TimeUnit.DAYS.toMillis(10), fixture.getTTLMs());
    assertArrayEquals(expected, fixture.getServerAddresses());
  }

  @Test
  public void shouldParseConfigurationUsingDefaults() {
    parameters.put(HOSTNAMES, "10.5.5.27");
    parameters.remove(INDEX_NAME);
    parameters.remove(INDEX_TYPE);
    parameters.remove(CLUSTER_NAME);

    fixture = new ElasticSearchSink();
    fixture.configure(new Context(parameters));

    String[] expected = { "10.5.5.27" };

    assertEquals(DEFAULT_INDEX_NAME, fixture.getIndexName());
    assertEquals(DEFAULT_INDEX_TYPE, fixture.getIndexType());
    assertEquals(DEFAULT_CLUSTER_NAME, fixture.getClusterName());
    assertArrayEquals(expected, fixture.getServerAddresses());
  }

  @Test
  public void shouldParseMultipleHostUsingDefaultPorts() {
    parameters.put(HOSTNAMES, "10.5.5.27,10.5.5.28,10.5.5.29");

    fixture = new ElasticSearchSink();
    fixture.configure(new Context(parameters));

    String[] expected = { "10.5.5.27", "10.5.5.28", "10.5.5.29" };

    assertArrayEquals(expected, fixture.getServerAddresses());
  }

  @Test
  public void shouldParseMultipleHostWithWhitespacesUsingDefaultPorts() {
    parameters.put(HOSTNAMES, " 10.5.5.27 , 10.5.5.28 , 10.5.5.29 ");

    fixture = new ElasticSearchSink();
    fixture.configure(new Context(parameters));

    String[] expected = { "10.5.5.27", "10.5.5.28", "10.5.5.29" };

    assertArrayEquals(expected, fixture.getServerAddresses());
  }

  @Test
  public void shouldParseMultipleHostAndPorts() {
    parameters.put(HOSTNAMES, "10.5.5.27:9300,10.5.5.28:9301,10.5.5.29:9302");

    fixture = new ElasticSearchSink();
    fixture.configure(new Context(parameters));

    String[] expected = { "10.5.5.27:9300", "10.5.5.28:9301", "10.5.5.29:9302" };

    assertArrayEquals(expected, fixture.getServerAddresses());
  }

  @Test
  public void shouldParseMultipleHostAndPortsWithWhitespaces() {
    parameters.put(HOSTNAMES,
        " 10.5.5.27 : 9300 , 10.5.5.28 : 9301 , 10.5.5.29 : 9302 ");

    fixture = new ElasticSearchSink();
    fixture.configure(new Context(parameters));

    String[] expected = { "10.5.5.27:9300", "10.5.5.28:9301", "10.5.5.29:9302" };

    assertArrayEquals(expected, fixture.getServerAddresses());
  }

  @Test
  public void shouldAllowCustomElasticSearchIndexRequestBuilderFactory()
      throws Exception {
    parameters.put(SERIALIZER,
        CustomElasticSearchIndexRequestBuilderFactory.class.getName());

    fixture.configure(new Context(parameters));

    Channel channel = bindAndStartChannel(fixture);
    Transaction tx = channel.getTransaction();
    tx.begin();
    String body = "{ foo: \"bar\" }";
    Event event = EventBuilder.withBody(body.getBytes());
    channel.put(event);
    tx.commit();
    tx.close();

    fixture.process();
    fixture.stop();

    assertEquals(fixture.getIndexName() + "-05_17_36_789",
        CustomElasticSearchIndexRequestBuilderFactory.actualIndexName);
    assertEquals(fixture.getIndexType(),
        CustomElasticSearchIndexRequestBuilderFactory.actualIndexType);
    assertArrayEquals(event.getBody(),
        CustomElasticSearchIndexRequestBuilderFactory.actualEventBody);
    assertTrue(CustomElasticSearchIndexRequestBuilderFactory.hasContext);
  }

  @Test
  public void shouldParseFullyQualifiedTTLs() {
    Map<String, Long> testTTLMap = new HashMap<String, Long>();
    testTTLMap.put("1ms", Long.valueOf(1));
    testTTLMap.put("1s", Long.valueOf(1000));
    testTTLMap.put("1m", Long.valueOf(60000));
    testTTLMap.put("1h", Long.valueOf(3600000));
    testTTLMap.put("1d", Long.valueOf(86400000));
    testTTLMap.put("1w", Long.valueOf(604800000));
    testTTLMap.put("1", Long.valueOf(86400000));

    parameters.put(HOSTNAMES, "10.5.5.27");
    parameters.put(CLUSTER_NAME, "testing-cluster-name");
    parameters.put(INDEX_NAME, "testing-index-name");
    parameters.put(INDEX_TYPE, "testing-index-type");

    for (String ttl : testTTLMap.keySet()) {
      parameters.put(TTL, ttl);
      fixture = new ElasticSearchSink();
      fixture.configure(new Context(parameters));

      String[] expected = { "10.5.5.27" };
      assertEquals("testing-cluster-name", fixture.getClusterName());
      assertEquals("testing-index-name", fixture.getIndexName());
      assertEquals("testing-index-type", fixture.getIndexType());
      assertEquals((long) testTTLMap.get(ttl), fixture.getTTLMs());
      assertArrayEquals(expected, fixture.getServerAddresses());

    }
  }

  public static final class CustomElasticSearchIndexRequestBuilderFactory
      extends AbstractElasticSearchIndexRequestBuilderFactory {

    static String actualIndexName, actualIndexType;
    static byte[] actualEventBody;
    static boolean hasContext;

    public CustomElasticSearchIndexRequestBuilderFactory() {
      super(FastDateFormat.getInstance("HH_mm_ss_SSS",
          TimeZone.getTimeZone("EST5EDT")));
    }

    @Override
    protected void prepareIndexRequest(IndexRequestBuilder indexRequest,
        String indexName, String indexType, Event event) throws IOException {
      actualIndexName = indexName;
      actualIndexType = indexType;
      actualEventBody = event.getBody();
      indexRequest.setIndex(indexName).setType(indexType)
          .setSource(event.getBody());
    }

    @Override
    public void configure(Context arg0) {
      hasContext = true;
    }

    @Override
    public void configure(ComponentConfiguration arg0) {
      //no-op
    }
  }

  @Test
  public void shouldFailToConfigureWithInvalidSerializerClass()
      throws Exception {

    parameters.put(SERIALIZER, "java.lang.String");
    try {
      Configurables.configure(fixture, new Context(parameters));
    } catch (ClassCastException e) {
      // expected
    }

    parameters.put(SERIALIZER, FakeConfigurable.class.getName());
    try {
      Configurables.configure(fixture, new Context(parameters));
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void shouldUseSpecifiedSerializer() throws Exception {
    Context context = new Context();
    context.put(SERIALIZER,
        "org.apache.flume.sink.elasticsearch.FakeEventSerializer");

    assertNull(fixture.getEventSerializer());
    fixture.configure(context);
    assertTrue(fixture.getEventSerializer() instanceof FakeEventSerializer);
  }

  @Test
  public void shouldUseSpecifiedIndexNameBuilder() throws Exception {
    Context context = new Context();
    context.put(ElasticSearchSinkConstants.INDEX_NAME_BUILDER,
            "org.apache.flume.sink.elasticsearch.FakeIndexNameBuilder");

    assertNull(fixture.getIndexNameBuilder());
    fixture.configure(context);
    assertTrue(fixture.getIndexNameBuilder() instanceof FakeIndexNameBuilder);
  }

  public static class FakeConfigurable implements Configurable {
    @Override
    public void configure(Context arg0) {
      // no-op
    }
  }
}

/**
 * Internal class. Fake event serializer used for tests
 */
class FakeEventSerializer implements ElasticSearchEventSerializer {

  static final byte[] FAKE_BYTES = new byte[] { 9, 8, 7, 6 };
  boolean configuredWithContext, configuredWithComponentConfiguration;

  @Override
  public BytesStream getContentBuilder(Event event) throws IOException {
    FastByteArrayOutputStream fbaos = new FastByteArrayOutputStream(4);
    fbaos.write(FAKE_BYTES);
    return fbaos;
  }

  @Override
  public void configure(Context arg0) {
    configuredWithContext = true;
  }

  @Override
  public void configure(ComponentConfiguration arg0) {
    configuredWithComponentConfiguration = true;
  }
}

/**
 * Internal class. Fake index name builder used only for tests.
 */
class FakeIndexNameBuilder implements IndexNameBuilder {

  static final String INDEX_NAME = "index_name";

  @Override
  public String getIndexName(Event event) {
    return INDEX_NAME;
  }

  @Override
  public String getIndexPrefix(Event event) {
    return INDEX_NAME;
  }

  @Override
  public void configure(Context context) {
  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }
}
