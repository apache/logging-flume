/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.source.pubsub;

public class CloudPubSubSourceContstants {
  public static final String SUBSCRIPTION = "subscription";
  public static final String SERVICE_ACCOUNT_KEY_PATH = "serviceAccountKeyFile";
  public static final String ACCOUNT_ID = "accountId";
  public static final String BATCH_SIZE = "batchSize";
  public static final String CONNECT_TIMEOUT = "connectTimeout";
  public static final String READ_TIMEOUT = "readTimeout";
  public static final int DEFAULT_BATCH_SIZE = 100;
  public static final int DEFAULT_RETRY_INTERVAL = 500;
  public static final int MAX_SLEEP_INTERVAL = 10000;
  public static final int DEFAULT_CONNECT_TIMEOUT = 20000;
  public static final int DEFAULT_READ_TIMEOUT = 20000;
}
