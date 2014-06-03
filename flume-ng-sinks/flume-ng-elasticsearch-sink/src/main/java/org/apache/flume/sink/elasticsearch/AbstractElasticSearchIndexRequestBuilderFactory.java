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

import java.io.IOException;

import org.apache.commons.lang.time.FastDateFormat;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;
import org.apache.flume.formatter.output.BucketPath;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;

import com.google.common.annotations.VisibleForTesting;

/**
 * Abstract base class for custom implementations of
 * {@link ElasticSearchIndexRequestBuilderFactory}.
 */
public abstract class AbstractElasticSearchIndexRequestBuilderFactory
    implements ElasticSearchIndexRequestBuilderFactory {

  /**
   * {@link FastDateFormat} to use for index names
   *   in {@link #getIndexName(String, long)}
   */
  protected final FastDateFormat fastDateFormat;

  /**
   * Constructor for subclasses
   * @param fastDateFormat {@link FastDateFormat} to use for index names
   */
  protected AbstractElasticSearchIndexRequestBuilderFactory(
    FastDateFormat fastDateFormat) {
    this.fastDateFormat = fastDateFormat;
  }

  /**
   * @see Configurable
   */
  @Override
  public abstract void configure(Context arg0);

  /**
   * @see ConfigurableComponent
   */
  @Override
  public abstract void configure(ComponentConfiguration arg0);

  /**
   * Creates and prepares an {@link IndexRequestBuilder} from the supplied
   * {@link Client} via delegation to the subclass-hook template methods
   * {@link #getIndexName(String, long)} and
   * {@link #prepareIndexRequest(IndexRequestBuilder, String, String, Event)}
   */
  @Override
  public IndexRequestBuilder createIndexRequest(Client client,
        String indexPrefix, String indexType, Event event) throws IOException {
    IndexRequestBuilder request = prepareIndex(client);
    String realIndexPrefix = BucketPath.escapeString(indexPrefix, event.getHeaders());
    String realIndexType = BucketPath.escapeString(indexType, event.getHeaders());

    TimestampedEvent timestampedEvent = new TimestampedEvent(event);
    long timestamp = timestampedEvent.getTimestamp();

    String indexName = getIndexName(realIndexPrefix, timestamp);
    prepareIndexRequest(request, indexName, realIndexType, timestampedEvent);
    return request;
  }

  @VisibleForTesting
  IndexRequestBuilder prepareIndex(Client client) {
    return client.prepareIndex();
  }

  /**
   * Gets the name of the index to use for an index request
   * @return index name of the form 'indexPrefix-formattedTimestamp'
   * @param indexPrefix
   *          Prefix of index name to use -- as configured on the sink
   * @param timestamp
   *          timestamp (millis) to format / use
   */
  protected String getIndexName(String indexPrefix, long timestamp) {
    return new StringBuilder(indexPrefix).append('-')
      .append(fastDateFormat.format(timestamp)).toString();
  }

  /**
   * Prepares an ElasticSearch {@link IndexRequestBuilder} instance
   * @param indexRequest
   *          The (empty) ElasticSearch {@link IndexRequestBuilder} to prepare
   * @param indexName
   *          Index name to use -- as per {@link #getIndexName(String, long)}
   * @param indexType
   *          Index type to use -- as configured on the sink
   * @param event
   *          Flume event to serialize and add to index request
   * @throws IOException
   *           If an error occurs e.g. during serialization
  */
  protected abstract void prepareIndexRequest(
      IndexRequestBuilder indexRequest, String indexName,
      String indexType, Event event) throws IOException;

}