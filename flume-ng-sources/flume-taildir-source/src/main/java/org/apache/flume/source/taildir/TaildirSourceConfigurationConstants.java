/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.flume.source.taildir;

public class TaildirSourceConfigurationConstants {
  /** Mapping for tailing file groups. */
  public static final String FILE_GROUPS = "filegroups";
  public static final String FILE_GROUPS_PREFIX = FILE_GROUPS + ".";

  /** Mapping for putting headers to events grouped by file groups. */
  public static final String HEADERS_PREFIX = "headers.";

  /** Path of position file. */
  public static final String POSITION_FILE = "positionFile";
  public static final String DEFAULT_POSITION_FILE = "/.flume/taildir_position.json";

  /** What size to batch with before sending to ChannelProcessor. */
  public static final String BATCH_SIZE = "batchSize";
  public static final int DEFAULT_BATCH_SIZE = 100;

  /** Whether to skip the position to EOF in the case of files not written on the position file. */
  public static final String SKIP_TO_END = "skipToEnd";
  public static final boolean DEFAULT_SKIP_TO_END = false;

  /** Time (ms) to close idle files. */
  public static final String IDLE_TIMEOUT = "idleTimeout";
  public static final int DEFAULT_IDLE_TIMEOUT = 120000;

  /** Whether to add the byte offset of a tailed line to the header */
  public static final String BYTE_OFFSET_HEADER = "byteOffsetHeader";
  public static final String BYTE_OFFSET_HEADER_KEY = "byteoffset";
  public static final boolean DEFAULT_BYTE_OFFSET_HEADER = false;

  /** Whether to cache the list of files matching the specified file patterns till parent directory
   * is modified.
   */
  public static final String CACHE_PATTERN_MATCHING = "cachePatternMatching";
  public static final boolean DEFAULT_CACHE_PATTERN_MATCHING = true;

  /** Header in which to put absolute path filename. */
  public static final String FILENAME_HEADER_KEY = "fileHeaderKey";
  public static final String DEFAULT_FILENAME_HEADER_KEY = "file";

  /** Whether to include absolute path filename in a header. */
  public static final String FILENAME_HEADER = "fileHeader";
  public static final boolean DEFAULT_FILE_HEADER = false;
}
