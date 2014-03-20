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
import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.BytesStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;

import java.io.IOException;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.bytes.BytesArray;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestElasticSearchRestClient {

  private ElasticSearchRestClient fixture;

  @Mock
  private ElasticSearchEventSerializer serializer;

  @Mock
  private IndexNameBuilder nameBuilder;
  
  @Mock
  private Event event;

  @Mock
  private HttpClient httpClient;

  @Mock
  private HttpResponse httpResponse;

  @Mock
  private StatusLine httpStatus;

  @Mock
  private HttpEntity httpEntity;

  private static final String INDEX_NAME = "foo_index";
  private static final String MESSAGE_CONTENT = "{\"body\":\"test\"}";
  private static final String[] HOSTS = {"host1", "host2"};

  @Before
  public void setUp() throws IOException {
    initMocks(this);
    BytesReference bytesReference = mock(BytesReference.class);
    BytesStream bytesStream = mock(BytesStream.class);

    when(nameBuilder.getIndexName(any(Event.class))).thenReturn(INDEX_NAME);
    when(bytesReference.toBytesArray()).thenReturn(new BytesArray(MESSAGE_CONTENT));
    when(bytesStream.bytes()).thenReturn(bytesReference);
    when(serializer.getContentBuilder(any(Event.class))).thenReturn(bytesStream);
    fixture = new ElasticSearchRestClient(HOSTS, serializer, httpClient);
  }

  @Test
  public void shouldAddNewEventWithoutTTL() throws Exception {
    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);

    when(httpStatus.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(httpResponse.getStatusLine()).thenReturn(httpStatus);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(httpResponse);
    
    fixture.addEvent(event, nameBuilder, "bar_type", -1);
    fixture.execute();

    verify(httpClient).execute(isA(HttpUriRequest.class));
    verify(httpClient).execute(argument.capture());

    assertEquals("http://host1/_bulk", argument.getValue().getURI().toString());
    assertEquals("{\"index\":{\"_type\":\"bar_type\",\"_index\":\"foo_index\"}}\n" + MESSAGE_CONTENT + "\n",
            EntityUtils.toString(argument.getValue().getEntity()));
  }

  @Test
  public void shouldAddNewEventWithTTL() throws Exception {
    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);

    when(httpStatus.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(httpResponse.getStatusLine()).thenReturn(httpStatus);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(httpResponse);

    fixture.addEvent(event, nameBuilder, "bar_type", 123);
    fixture.execute();

    verify(httpClient).execute(isA(HttpUriRequest.class));
    verify(httpClient).execute(argument.capture());

    assertEquals("http://host1/_bulk", argument.getValue().getURI().toString());
    assertEquals("{\"index\":{\"_type\":\"bar_type\",\"_index\":\"foo_index\",\"_ttl\":\"123\"}}\n" +
            MESSAGE_CONTENT + "\n", EntityUtils.toString(argument.getValue().getEntity()));
  }

  @Test(expected = EventDeliveryException.class)
  public void shouldThrowEventDeliveryException() throws Exception {
    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);

    when(httpStatus.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    when(httpResponse.getStatusLine()).thenReturn(httpStatus);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(httpResponse);

    fixture.addEvent(event, nameBuilder, "bar_type", 123);
    fixture.execute();
  }

  @Test()
  public void shouldRetryBulkOperation() throws Exception {
    ArgumentCaptor<HttpPost> argument = ArgumentCaptor.forClass(HttpPost.class);

    when(httpStatus.getStatusCode()).thenReturn(HttpStatus.SC_INTERNAL_SERVER_ERROR, HttpStatus.SC_OK);
    when(httpResponse.getStatusLine()).thenReturn(httpStatus);
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(httpResponse);

    fixture.addEvent(event, nameBuilder, "bar_type", 123);
    fixture.execute();

    verify(httpClient, times(2)).execute(isA(HttpUriRequest.class));
    verify(httpClient, times(2)).execute(argument.capture());

    List<HttpPost> allValues = argument.getAllValues();
    assertEquals("http://host1/_bulk", allValues.get(0).getURI().toString());
    assertEquals("http://host2/_bulk", allValues.get(1).getURI().toString());
  }
}
