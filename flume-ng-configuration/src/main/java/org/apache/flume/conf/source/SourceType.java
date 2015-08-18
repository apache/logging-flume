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
package org.apache.flume.conf.source;

/**
 * Enumeration of built in source types available in the system.
 */
public enum SourceType {

  /**
   * Place holder for custom sources not part of this enumeration.
   */
  OTHER(null),

  /**
   * Sequence generator file source.
   *
   * @see org.apache.flume.source.SequenceGeneratorSource
   */
  SEQ("org.apache.flume.source.SequenceGeneratorSource"),

  /**
   * Netcat source.
   *
   * @see org.apache.flume.source.NetcatSource
   */
  NETCAT("org.apache.flume.source.NetcatSource"),

  /**
   * Exec source.
   *
   * @see org.apache.flume.source.ExecSource
   */
  EXEC("org.apache.flume.source.ExecSource"),

  /**
   * Avro source.
   *
   * @see org.apache.flume.source.AvroSource
   */
  AVRO("org.apache.flume.source.AvroSource"),

  /**
   * SyslogTcpSource
   *
   * @see org.apache.flume.source.SyslogTcpSource
   */
  SYSLOGTCP("org.apache.flume.source.SyslogTcpSource"),

  /**
   * MultiportSyslogTCPSource
   *
   * @see org.apache.flume.source.MultiportSyslogTCPSource
   */
  MULTIPORT_SYSLOGTCP("org.apache.flume.source.MultiportSyslogTCPSource"),

  /**
   * SyslogUDPSource
   *
   * @see org.apache.flume.source.SyslogUDPSource
   */
  SYSLOGUDP("org.apache.flume.source.SyslogUDPSource"),

  /**
   * Spool directory source
   *
   * @see org.apache.flume.source.SpoolDirectorySource
   */
  SPOOLDIR("org.apache.flume.source.SpoolDirectorySource"),

  /**
   * HTTP Source
   *
   * @see org.apache.flume.source.http.HTTPSource
   */
  HTTP("org.apache.flume.source.http.HTTPSource"),

  /**
   * Thrift Source
   *
   * @see org.apache.flume.source.ThriftSource
   */
  THRIFT("org.apache.flume.source.ThriftSource"),

  /**
   * JMS Source
   *
   * @see org.apache.flume.source.jms.JMSSource
   */
  JMS("org.apache.flume.source.jms.JMSSource"),

  /**
   * Taildir Source
   *
   * @see org.apache.flume.source.taildir.TaildirSource
   */
  TAILDIR("org.apache.flume.source.taildir.TaildirSource")
  ;

  private final String sourceClassName;

  private SourceType(String sourceClassName) {
    this.sourceClassName = sourceClassName;
  }

  public String getSourceClassName() {
    return sourceClassName;
  }
}
