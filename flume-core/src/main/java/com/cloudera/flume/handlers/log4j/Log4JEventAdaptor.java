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
package com.cloudera.flume.handlers.log4j;

import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

import com.cloudera.flume.core.EventBaseImpl;
import com.cloudera.util.NetUtils;

/**
 * This is an adaptor for converting the log4j logging interface into flumes.
 * 
 * This is assumed to only be present in the instrumented process, so getting
 * local host is reasonable for getting the host field.
 * 
 * This is not threadsafe.
 */
public class Log4JEventAdaptor extends EventBaseImpl {

  LoggingEvent evt;
  long nanos;

  public Log4JEventAdaptor(LoggingEvent evt) {
    super();
    this.evt = evt;
    // This is needed to differentiate between events at the same millisecond.
    this.nanos = System.nanoTime();
  }

  public byte[] getBody() {
    return evt.getRenderedMessage().getBytes();
  }

  public Priority getPriority() {
    return level2prio(evt.getLevel());
  }

  public static Priority level2prio(Level l) {
    if (l == Level.DEBUG)
      return Priority.DEBUG;
    if (l == Level.INFO)
      return Priority.INFO;
    if (l == Level.WARN)
      return Priority.WARN;
    if (l == Level.ERROR)
      return Priority.ERROR;
    if (l == Level.FATAL)
      return Priority.FATAL;

    // default to info level
    return Priority.INFO;
  }

  public long getTimestamp() {
    return evt.getTimeStamp();
  }

  @Override
  public long getNanos() {
    return nanos;
  }

  @Override
  public String getHost() {
    return NetUtils.localhost();
  }

}
