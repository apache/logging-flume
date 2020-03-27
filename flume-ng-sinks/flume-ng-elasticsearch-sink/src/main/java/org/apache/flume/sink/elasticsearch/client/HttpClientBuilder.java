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

import org.apache.flume.Context;
import org.apache.flume.conf.Configurable;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * HTTP client which is used for sending bulks of events to ElasticSearch using
 * ElasticSearch HTTP API.  This is configurable, so any config params required
 * should be taken through this.
 *
 * You can extend this class in your own Flume plug-ins to customize HTTP
 * request and response processing (e.g. to handle authentication).
 */
public class HttpClientBuilder implements Configurable {

  /**
   * Create the HTTP client.
   */
  public HttpClient newHttpClient() {
    return new DefaultHttpClient();
  }

  @Override
  public void configure(Context context) {
    // NO-OP...
  }
}
