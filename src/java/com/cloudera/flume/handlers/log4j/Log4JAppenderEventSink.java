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

import java.io.IOException;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import com.cloudera.flume.core.EventSink;

/**
 * Ideally these reliable mechanisms will be decoupled from here so they can be
 * used by other event sources.
 */
public class Log4JAppenderEventSink extends AppenderSkeleton {

  EventSink sink;

  /**
   * Default constructor does nothing.
   * 
   * @param sink
   */
  public Log4JAppenderEventSink() {
  }

  public Log4JAppenderEventSink(EventSink sink) {
    this.sink = sink;
  }

  public void setEventSink(EventSink sink) {
    assert (sink != null);
    this.sink = sink;
  }

  public EventSink getEventSink() {
    return sink;
  }

  @Override
  protected void append(LoggingEvent event) {
    try {
      sink.append(new Log4JEventAdaptor(event));
    } catch (IOException e) {
      throw new RuntimeException("TODO, log event IO exception");
    }
  }

  public void close() {
    try {
      sink.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public boolean requiresLayout() {
    return false;
  }

}
