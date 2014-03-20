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
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.elasticsearch.IndexNameBuilder;

/**
 * Interface for an ElasticSearch client which is responsible for sending bulks
 * of events to ElasticSearch.
 */
public interface ElasticSearchClient extends Configurable {

  /**
   * Close connection to elastic search in client
   */
  void close();

  /**
   * Add new event to the bulk
   *
   * @param event
   *    Flume Event
   * @param indexNameBuilder
   *    Index name builder which generates name of index to feed
   * @param indexType
   *    Name of type of document which will be sent to the elasticsearch cluster
   * @param ttlMs
   *    Time to live expressed in milliseconds. Value <= 0 is ignored
   * @throws Exception
   */
  public void addEvent(Event event, IndexNameBuilder indexNameBuilder,
      String indexType, long ttlMs) throws Exception;

  /**
   * Sends bulk to the elasticsearch cluster
   *
   * @throws Exception
   */
  void execute() throws Exception;
}
