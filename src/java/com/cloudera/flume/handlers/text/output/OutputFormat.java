/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.handlers.text.output;

import java.io.IOException;
import java.io.OutputStream;

import com.cloudera.flume.core.Event;

/**
 * Output formats take an Event and a OutputStream and then formats the event
 * before writing to the OutputStream. There are many potential formats. Ideally
 * text formats would revert an event back to exactly the version found in a
 * text log file. (an apache one would dump in apache format, syslog would dump
 * in the same format as syslog.log, etc) Oftentimes this is not possible and a
 * best effort version is output.
 * 
 * Requiring the outputStream as an arg allows for streaming writes and removes
 * the potential requirement for copies of large memory buffers with large
 * events. It also allow sophisticated (avro, seqfile) output formats to fancy
 * things such as markers for bundling/splitability, adding error correction
 * codes, checksums, etc.
 */
public interface OutputFormat {

  /**
   * Outputs formatted event to the specified output stream.
   */
  public void format(OutputStream o, Event e) throws IOException;

  /**
   * A constant name for this particular outputFormat
   */
  public String getFormatName();
}
