/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.kite;

import org.kitesdk.data.URIBuilder;

public class DatasetSinkConstants {
  /**
   * URI of the Kite Dataset
   */
  public static final String CONFIG_KITE_DATASET_URI = "kite.dataset.uri";

  /**
   * URI of the Kite DatasetRepository.
   */
  public static final String CONFIG_KITE_REPO_URI = "kite.repo.uri";

  /**
   * Name of the Kite Dataset to write into.
   */
  public static final String CONFIG_KITE_DATASET_NAME = "kite.dataset.name";

  /**
   * Namespace of the Kite Dataset to write into.
   */
  public static final String CONFIG_KITE_DATASET_NAMESPACE =
      "kite.dataset.namespace";
  public static final String DEFAULT_NAMESPACE = URIBuilder.NAMESPACE_DEFAULT;

  /**
   * Number of records to process from the incoming channel per call to process.
   */
  public static final String CONFIG_KITE_BATCH_SIZE = "kite.batchSize";
  public static long DEFAULT_BATCH_SIZE = 100;

  /**
   * Maximum time to wait before finishing files.
   */
  public static final String CONFIG_KITE_ROLL_INTERVAL = "kite.rollInterval";
  public static int DEFAULT_ROLL_INTERVAL = 30; // seconds

  /**
   * Flag for committing the Flume transaction on each batch for Flushable
   * datasets. When set to false, Flume will only commit the transaction when
   * roll interval has expired. Setting this to false requires enough space
   * in the channel to handle all events delivered during the roll interval.
   * Defaults to true.
   */
  public static final String CONFIG_FLUSHABLE_COMMIT_ON_BATCH =
      "kite.flushable.commiteOnBatch";
  public static boolean DEFAULT_FLUSHABLE_COMMIT_ON_BATCH = true;

  /**
   * Flag for syncing the DatasetWriter on each batch for Syncable
   * datasets. Defaults to true.
   */
  public static final String CONFIG_SYNCABLE_SYNC_ON_BATCH =
      "kite.syncable.syncOnBatch";
  public static boolean DEFAULT_SYNCABLE_SYNC_ON_BATCH = true;

  /**
   * Parser used to parse Flume Events into Kite entities.
   */
  public static final String CONFIG_ENTITY_PARSER = "kite.entityParser";

  /**
   * Built-in entity parsers
   */
  public static final String AVRO_ENTITY_PARSER = "avro";
  public static final String DEFAULT_ENTITY_PARSER = AVRO_ENTITY_PARSER;
  public static final String[] AVAILABLE_PARSERS = new String[] {
    AVRO_ENTITY_PARSER
  };

  /**
   * Policy used to handle non-recoverable failures.
   */
  public static final String CONFIG_FAILURE_POLICY = "kite.failurePolicy";

  /**
   * Write non-recoverable Flume events to a Kite dataset.
   */
  public static final String SAVE_FAILURE_POLICY = "save";

  /**
   * The URI to write non-recoverable Flume events to in the case of an error.
   * If the dataset doesn't exist, it will be created.
   */
  public static final String CONFIG_KITE_ERROR_DATASET_URI =
      "kite.error.dataset.uri";

  /**
   * Retry non-recoverable Flume events. This will lead to a never ending cycle
   * of failure, but matches the previous default semantics of the DatasetSink.
   */
  public static final String RETRY_FAILURE_POLICY = "retry";
  public static final String DEFAULT_FAILURE_POLICY = RETRY_FAILURE_POLICY;
  public static final String[] AVAILABLE_POLICIES = new String[] {
    RETRY_FAILURE_POLICY,
    SAVE_FAILURE_POLICY
  };

  /**
   * Headers where avro schema information is expected.
   */
  public static final String AVRO_SCHEMA_LITERAL_HEADER =
      "flume.avro.schema.literal";
  public static final String AVRO_SCHEMA_URL_HEADER = "flume.avro.schema.url";

  /**
   * Hadoop authentication settings
   */
  public static final String AUTH_PROXY_USER = "auth.proxyUser";
  public static final String AUTH_PRINCIPAL = "auth.kerberosPrincipal";
  public static final String AUTH_KEYTAB = "auth.kerberosKeytab";
}
