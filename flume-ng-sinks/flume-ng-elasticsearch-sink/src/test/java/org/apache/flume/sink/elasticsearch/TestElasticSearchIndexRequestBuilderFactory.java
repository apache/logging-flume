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

import com.google.common.collect.Maps;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.conf.sink.SinkConfiguration;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.sink.elasticsearch.legacy.FastByteArrayOutputStream;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.io.BytesStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

import static org.junit.Assert.*;

public class TestElasticSearchIndexRequestBuilderFactory
    extends AbstractElasticSearchSinkTest {

  private static Client FAKE_CLIENT = null;
  private EventSerializerIndexRequestBuilderFactory factory;

  private FakeEventSerializer serializer;

  @Before
  public void setupFactory() throws Exception {

    serializer = new FakeEventSerializer();
    factory = new EventSerializerIndexRequestBuilderFactory(serializer) {
      @Override
      IndexRequestBuilder prepareIndex(Client client) {
        Settings settings = Settings.builder()
            .put("path.data", "target/es-test")
            .put("path.home", "D:\\dev\\elasticsearch\\elasticsearch-5.2.1")
            .build();

        try {
          FAKE_CLIENT = new PreBuiltTransportClient(settings)
              .addTransportAddress(
                  new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
          ;
        } catch (UnknownHostException e) {
          e.printStackTrace();
        }

        return new IndexRequestBuilder(FAKE_CLIENT, IndexAction.INSTANCE);
      }
    };
  }

  @Test
  public void shouldUseUtcAsBasisForDateFormat() {
    String timeZone = factory.fastDateFormat.getTimeZone().getDisplayName();
    assertTrue("Temps universel coordonné".equals(timeZone) ||
        "Coordinated Universal Time".equals(timeZone));
  }

  @Test
  public void indexNameShouldBePrefixDashFormattedTimestamp() {
    long millis = 987654321L;
    assertEquals("prefix-" + factory.fastDateFormat.format(millis),
        factory.getIndexName("prefix", millis));
  }

  @Test
  public void shouldEnsureTimestampHeaderPresentInTimestampedEvent() {
    SimpleEvent base = new SimpleEvent();

    TimestampedEvent timestampedEvent = new TimestampedEvent(base);
    assertEquals(FIXED_TIME_MILLIS, timestampedEvent.getTimestamp());
    assertEquals(String.valueOf(FIXED_TIME_MILLIS),
        timestampedEvent.getHeaders().get("timestamp"));
  }

  @Test
  public void shouldUseExistingTimestampHeaderInTimestampedEvent() {
    SimpleEvent base = new SimpleEvent();
    Map<String, String> headersWithTimestamp = Maps.newHashMap();
    headersWithTimestamp.put("timestamp", "-321");
    base.setHeaders(headersWithTimestamp);

    TimestampedEvent timestampedEvent = new TimestampedEvent(base);
    assertEquals(-321L, timestampedEvent.getTimestamp());
    assertEquals("-321", timestampedEvent.getHeaders().get("timestamp"));
  }

  @Test
  public void shouldUseExistingAtTimestampHeaderInTimestampedEvent() {
    SimpleEvent base = new SimpleEvent();
    Map<String, String> headersWithTimestamp = Maps.newHashMap();
    headersWithTimestamp.put("@timestamp", "-999");
    base.setHeaders(headersWithTimestamp);

    TimestampedEvent timestampedEvent = new TimestampedEvent(base);
    assertEquals(-999L, timestampedEvent.getTimestamp());
    assertEquals("-999", timestampedEvent.getHeaders().get("@timestamp"));
    assertNull(timestampedEvent.getHeaders().get("timestamp"));
  }

  @Test
  public void shouldPreserveBodyAndNonTimestampHeadersInTimestampedEvent() {
    SimpleEvent base = new SimpleEvent();
    base.setBody(new byte[] {1, 2, 3, 4});
    Map<String, String> headersWithTimestamp = Maps.newHashMap();
    headersWithTimestamp.put("foo", "bar");
    base.setHeaders(headersWithTimestamp);

    TimestampedEvent timestampedEvent = new TimestampedEvent(base);
    assertEquals("bar", timestampedEvent.getHeaders().get("foo"));
    assertArrayEquals(base.getBody(), timestampedEvent.getBody());
  }

  @Test
  public void shouldSetIndexNameTypeAndSerializedEventIntoIndexRequest() throws Exception {

    String indexPrefix = "qwerty";
    String indexType = "uiop";
    Event event = new SimpleEvent();

    IndexRequestBuilder indexRequestBuilder =
        factory.createIndexRequest(FAKE_CLIENT, indexPrefix, indexType, event);

    assertEquals(indexPrefix + '-'
            + ElasticSearchIndexRequestBuilderFactory.df.format(FIXED_TIME_MILLIS),
        indexRequestBuilder.request().index());
    assertEquals(indexType, indexRequestBuilder.request().type());

    assertArrayEquals(FakeEventSerializer.FAKE_BYTES,
        indexRequestBuilder.request().source().toBytesRef().bytes);
  }

  @Test
  public void shouldSetIndexNameFromTimestampHeaderWhenPresent() throws Exception {
    String indexPrefix = "qwerty";
    String indexType = "uiop";
    Event event = new SimpleEvent();
    event.getHeaders().put("timestamp", "1213141516");

    IndexRequestBuilder indexRequestBuilder = factory.createIndexRequest(
        null, indexPrefix, indexType, event);

    assertEquals(indexPrefix + '-'
            + ElasticSearchIndexRequestBuilderFactory.df.format(1213141516L),
        indexRequestBuilder.request().index());
  }

  @Test
  public void shouldSetIndexNameTypeFromHeaderWhenPresent() throws Exception {
    String indexPrefix = "%{index-name}";
    String indexType = "%{index-type}";
    String indexValue = "testing-index-name-from-headers";
    String typeValue = "testing-index-type-from-headers";

    Event event = new SimpleEvent();
    event.getHeaders().put("index-name", indexValue);
    event.getHeaders().put("index-type", typeValue);

    Settings settings = Settings.builder()
        .put("path.data", "target/es-test")
        .put("path.home", "D:\\dev\\elasticsearch\\elasticsearch-5.2.1")
        .build();

    try {
      FAKE_CLIENT = new PreBuiltTransportClient(settings)
          .addTransportAddress(
              new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));

      IndexRequestBuilder indexRequestBuilder =
          factory.createIndexRequest(FAKE_CLIENT, indexPrefix, indexType, event);

      assertEquals(indexValue + '-'
              + ElasticSearchIndexRequestBuilderFactory.df.format(FIXED_TIME_MILLIS),
          indexRequestBuilder.request().index());
      assertEquals(typeValue, indexRequestBuilder.request().type());
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }


  }

  @Test
  public void shouldConfigureEventSerializer() throws Exception {
    assertFalse(serializer.configuredWithContext);
    factory.configure(new Context());
    assertTrue(serializer.configuredWithContext);

    assertFalse(serializer.configuredWithComponentConfiguration);
    factory.configure(new SinkConfiguration("name"));
    assertTrue(serializer.configuredWithComponentConfiguration);
  }

  static class FakeEventSerializer implements ElasticSearchEventSerializer {

    static final byte[] FAKE_BYTES = new byte[] {9, 8, 7, 6};
    boolean configuredWithContext;
    boolean configuredWithComponentConfiguration;

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
}
