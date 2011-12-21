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

package org.apache.flume.sink.hdfs;

import java.io.IOException;

import org.apache.flume.sink.FlumeFormatter;

public class HDFSFormatterFactory {

  HDFSFormatterFactory() {

  }

  static final String hdfsWritableFormat = "Writable";
  static final String hdfsAvroFormat = "Avro";
  static final String hdfsTextFormat = "Text";

  static FlumeFormatter getFormatter(String formatType) throws IOException {
    if (formatType.equalsIgnoreCase(hdfsWritableFormat)) {
      return new HDFSWritableFormatter();
    } else if (formatType.equalsIgnoreCase(hdfsTextFormat)) {
      return new HDFSTextFormatter();
    } else {
      throw new IOException("Incorrect formatter type: " + formatType);
    }
  }
}
