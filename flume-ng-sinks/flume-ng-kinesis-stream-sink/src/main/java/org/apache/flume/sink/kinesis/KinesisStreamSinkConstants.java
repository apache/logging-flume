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
package org.apache.flume.sink.kinesis;

public class KinesisStreamSinkConstants {
  public static final String CONFIG_KEY_STREAM_NAME = "streamName";
  public static final String CONFIG_KEY_CREDENTIAL_TYPE = "credentialType";
  public static final String CONFIG_KEY_AWS_ACCESS_KEY = "accessKey";
  public static final String CONFIG_KEY_AWS_SECRET_KEY = "secretKey";
  public static final String CONFIG_KEY_AWS_SESSION_DURATION_SECONDS = "durationSeconds";
  public static final String CONFIG_KEY_AWS_REGION = "region";
  public static final String HEADER_KEY_PARTITION_KEY = "key";
  public static final String CONFIG_DEFAULT_VALUE_CONFIG_TYPE = CredentialType.none.name();
  public static final int CONFIG_DEFAULT_SESSION_DURATION_SECONDS = 3600;
}
