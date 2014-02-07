/*
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
package org.apache.flume.source.jms;

public class JMSSourceConfiguration {

  public static final String INITIAL_CONTEXT_FACTORY = "initialContextFactory";

  public static final String CONNECTION_FACTORY = "connectionFactory";
  public static final String CONNECTION_FACTORY_DEFAULT = "ConnectionFactory";

  public static final String PROVIDER_URL = "providerURL";

  public static final String DESTINATION_NAME = "destinationName";

  public static final String DESTINATION_TYPE = "destinationType";
  public static final String DESTINATION_LOCATOR = "destinationLocator";
  public static final String DESTINATION_LOCATOR_DEFAULT = "CDI";

  public static final String DESTINATION_TYPE_QUEUE = "queue";
  public static final String DESTINATION_TYPE_TOPIC = "topic";

  public static final String MESSAGE_SELECTOR = "messageSelector";

  public static final String USERNAME = "userName";

  public static final String PASSWORD_FILE = "passwordFile";

  public static final String BATCH_SIZE = "batchSize";
  public static final int BATCH_SIZE_DEFAULT = 100;

  public static final String ERROR_THRESHOLD = "errorThreshold";
  public static final int ERROR_THRESHOLD_DEFAULT = 10;

  public static final String POLL_TIMEOUT = "pollTimeout";
  public static final long POLL_TIMEOUT_DEFAULT = 1000L;

  public static final String CONVERTER = "converter";

  public static final String CONVERTER_TYPE = CONVERTER + ".type";
  public static final String CONVERTER_TYPE_DEFAULT = "DEFAULT";

  public static final String CONVERTER_CHARSET = CONVERTER + ".charset";
  public static final String CONVERTER_CHARSET_DEFAULT = "UTF-8";

}
