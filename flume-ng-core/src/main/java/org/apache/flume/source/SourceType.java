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
package org.apache.flume.source;


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
   * @see SequenceGeneratorSource
   */
  SEQ(SequenceGeneratorSource.class.getName()),

  /**
   * Netcat source.
   * @see NetcatSource
   */
  NETCAT(NetcatSource.class.getName()),

  /**
   * Exec source.
   * @see ExecSource
   */
  EXEC(ExecSource.class.getName()),

  /**
   * Avro soruce.
   * @see AvroSource
   */
  AVRO(AvroSource.class.getName());

  private final String sourceClassName;

  private SourceType(String sourceClassName) {
    this.sourceClassName = sourceClassName;
  }

  public String getSourceClassName() {
    return sourceClassName;
  }
}
