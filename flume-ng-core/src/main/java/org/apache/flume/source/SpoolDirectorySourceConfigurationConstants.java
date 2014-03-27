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

package org.apache.flume.source;

import org.apache.flume.serialization.DecodeErrorPolicy;

public class SpoolDirectorySourceConfigurationConstants {
  /** Directory where files are deposited. */
  public static final String SPOOL_DIRECTORY = "spoolDir";

  /** Suffix appended to files when they are finished being sent. */
  public static final String SPOOLED_FILE_SUFFIX = "fileSuffix";
  public static final String DEFAULT_SPOOLED_FILE_SUFFIX = ".COMPLETED";

  /** Header in which to put absolute path filename. */
  public static final String FILENAME_HEADER_KEY = "fileHeaderKey";
  public static final String DEFAULT_FILENAME_HEADER_KEY = "file";

  /** Whether to include absolute path filename in a header. */
  public static final String FILENAME_HEADER = "fileHeader";
  public static final boolean DEFAULT_FILE_HEADER = false;

  /** Header in which to put the basename of file. */
  public static final String BASENAME_HEADER_KEY = "basenameHeaderKey";
  public static final String DEFAULT_BASENAME_HEADER_KEY = "basename";

  /** Whether to include the basename of a file in a header. */
  public static final String BASENAME_HEADER = "basenameHeader";
  public static final boolean DEFAULT_BASENAME_HEADER = false;

  /** What size to batch with before sending to ChannelProcessor. */
  public static final String BATCH_SIZE = "batchSize";
  public static final int DEFAULT_BATCH_SIZE = 100;

  /** Maximum number of lines to buffer between commits. */
  @Deprecated
  public static final String BUFFER_MAX_LINES = "bufferMaxLines";
  @Deprecated
  public static final int DEFAULT_BUFFER_MAX_LINES = 100;

  /** Maximum length of line (in characters) in buffer between commits. */
  @Deprecated
  public static final String BUFFER_MAX_LINE_LENGTH = "bufferMaxLineLength";
  @Deprecated
  public static final int DEFAULT_BUFFER_MAX_LINE_LENGTH = 5000;

  /** Pattern of files to ignore */
  public static final String IGNORE_PAT = "ignorePattern";
  public static final String DEFAULT_IGNORE_PAT = "^$"; // no effect

  /** Directory to store metadata about files being processed */
  public static final String TRACKER_DIR = "trackerDir";
  public static final String DEFAULT_TRACKER_DIR = ".flumespool";

  /** Deserializer to use to parse the file data into Flume Events */
  public static final String DESERIALIZER = "deserializer";
  public static final String DEFAULT_DESERIALIZER = "LINE";

  public static final String DELETE_POLICY = "deletePolicy";
  public static final String DEFAULT_DELETE_POLICY = "never";

  /** Character set used when reading the input. */
  public static final String INPUT_CHARSET = "inputCharset";
  public static final String DEFAULT_INPUT_CHARSET = "UTF-8";

  /** What to do when there is a character set decoding error. */
  public static final String DECODE_ERROR_POLICY = "decodeErrorPolicy";
  public static final String DEFAULT_DECODE_ERROR_POLICY =
      DecodeErrorPolicy.FAIL.name();

  public static final String MAX_BACKOFF = "maxBackoff";

  public static final Integer DEFAULT_MAX_BACKOFF = 4000;
  
  /** Consume order. */
  public enum ConsumeOrder {
    OLDEST, YOUNGEST, RANDOM
  }
  public static final String CONSUME_ORDER = "consumeOrder";
  public static final ConsumeOrder DEFAULT_CONSUME_ORDER = ConsumeOrder.OLDEST;    
}
