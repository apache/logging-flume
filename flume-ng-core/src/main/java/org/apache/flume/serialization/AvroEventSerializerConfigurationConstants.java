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
package org.apache.flume.serialization;

import org.apache.avro.file.CodecFactory;

public class AvroEventSerializerConfigurationConstants {

  /**
   * Avro sync interval, in approximate bytes
   */
  public static final String SYNC_INTERVAL_BYTES = "syncIntervalBytes";
  public static final int DEFAULT_SYNC_INTERVAL_BYTES = 2048000; // 2MB

  /**
   * Avro compression codec. For supported codecs, see Avro's
   * {@link CodecFactory} class.
   */
  public static final String COMPRESSION_CODEC = "compressionCodec";
  public static final String DEFAULT_COMPRESSION_CODEC = "null"; // no codec

}
