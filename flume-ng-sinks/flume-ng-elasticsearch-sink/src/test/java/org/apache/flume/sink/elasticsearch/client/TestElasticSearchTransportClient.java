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

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.sink.elasticsearch.ContentBuilderUtil;
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStream;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestElasticSearchTransportClient {

  private ElasticSearchTransportClient fixture;

  @Mock
  private ElasticSearchEventSerializer serializer;

  @Mock
  private IndexNameBuilder nameBuilder;

  @Mock
  private Client elasticSearchClient;

  @Mock
  private BulkRequestBuilder bulkRequestBuilder;

  @Mock
  private IndexRequestBuilder indexRequestBuilder;

  @Mock
  private Event event;

  @Before
  public void setUp() throws IOException {
    initMocks(this);
    BytesReference bytesReference = mock(BytesReference.class);
    BytesStream bytesStream = mock(BytesStream.class);
    byte[] FAKE_BYTES = new byte[]{9, 8, 7, 6};
    XContentBuilder builder = jsonBuilder().startObject();
    ContentBuilderUtil.appendField(builder, "@message", FAKE_BYTES);
    builder.endObject();

    when(nameBuilder.getIndexName(any(Event.class))).thenReturn("foo_index");
    when(bytesReference.toBytesRef()).thenReturn(new BytesRef("{\"body\":\"test\"}".getBytes()));
    when(bytesStream.bytes()).thenReturn(bytesReference);
    when(serializer.getContentBuilder(any(Event.class)))
        .thenReturn(builder);
    when(elasticSearchClient.prepareIndex(anyString(), anyString()))
        .thenReturn(indexRequestBuilder);
    when(indexRequestBuilder.setSource(bytesReference)).thenReturn(
        indexRequestBuilder);

    fixture = new ElasticSearchTransportClient(elasticSearchClient, serializer);
    fixture.setBulkRequestBuilder(bulkRequestBuilder);
  }


  @Test
  public void shouldExecuteBulkRequestBuilder() throws Exception {
    ListenableActionFuture<BulkResponse> action =
        (ListenableActionFuture<BulkResponse>) mock(ListenableActionFuture.class);
    BulkResponse response = mock(BulkResponse.class);
    when(bulkRequestBuilder.execute()).thenReturn(action);
    when(action.actionGet()).thenReturn(response);
    when(response.hasFailures()).thenReturn(false);

    fixture.addEvent(event, nameBuilder, "bar_type");
    fixture.execute();
    verify(bulkRequestBuilder).execute();
  }

  @Test(expected = EventDeliveryException.class)
  public void shouldThrowExceptionOnExecuteFailed() throws Exception {
    ListenableActionFuture<BulkResponse> action =
        (ListenableActionFuture<BulkResponse>) mock(ListenableActionFuture.class);
    BulkResponse response = mock(BulkResponse.class);
    when(bulkRequestBuilder.execute()).thenReturn(action);
    when(action.actionGet()).thenReturn(response);
    when(response.hasFailures()).thenReturn(true);

    fixture.addEvent(event, nameBuilder, "bar_type");
    fixture.execute();
  }
}
