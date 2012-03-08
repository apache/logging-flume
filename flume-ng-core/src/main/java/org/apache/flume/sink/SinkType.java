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
package org.apache.flume.sink;


/**
 * Enumeration of built in sink types available in the system.
 */
public enum SinkType {

  /**
   * Place holder for custom sinks not part of this enumeration.
   */
  OTHER(null),


  /**
   * Null sink
   * @see NullSink
   */
  NULL(NullSink.class.getName()),

  /**
   * Logger sink
   * @see LoggerSink
   */
  LOGGER(LoggerSink.class.getName()),

  /**
   * Rolling file sink
   * @see RollingFileSink
   */
  FILE_ROLL(RollingFileSink.class.getName()),

  /**
   * HDFS Sink provided by org.apache.flume.sink.hdfs.HDFSEventSink
   */
  HDFS("org.apache.flume.sink.hdfs.HDFSEventSink"),

  /**
   * IRC Sink provided by org.apache.flume.sink.irc.IRCSink
   */ 
  IRC("org.apache.flume.sink.irc.IRCSink"),

  /**
   * Avro sink
   * @see AvroSink
   */
  AVRO(AvroSink.class.getName());

  private final String sinkClassName;

  private SinkType(String sinkClassName) {
    this.sinkClassName = sinkClassName;
  }

  public String getSinkClassName() {
    return sinkClassName;
  }

}
