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

import org.apache.flume.sink.elasticsearch.ElasticSearchEventSerializer;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import org.mockito.Mock;
import static org.mockito.MockitoAnnotations.initMocks;

public class TestElasticSearchClientFactory {

  ElasticSearchClientFactory factory;
  
  @Mock
  ElasticSearchEventSerializer serializer;

  @Before
  public void setUp() {
    initMocks(this);
    factory = new ElasticSearchClientFactory();
  }

  @Test
  public void shouldReturnTransportClient() throws Exception {
    String[] hostNames = { "127.0.0.1" };
    Object o = factory.getClient(ElasticSearchClientFactory.TransportClient,
            hostNames, "test", serializer, null);
    assertThat(o, instanceOf(ElasticSearchTransportClient.class));
  }

  @Test
  public void shouldReturnRestClient() throws NoSuchClientTypeException {
    String[] hostNames = { "127.0.0.1" };
    Object o = factory.getClient(ElasticSearchClientFactory.RestClient,
            hostNames, "test", serializer, null);
    assertThat(o, instanceOf(ElasticSearchRestClient.class));
  }

  @Test(expected=NoSuchClientTypeException.class)
  public void shouldThrowNoSuchClientTypeException() throws NoSuchClientTypeException {
    String[] hostNames = {"127.0.0.1"};
    factory.getClient("not_existing_client", hostNames, "test", null, null);
  }
}
