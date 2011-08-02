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
package com.cloudera.flume.handlers.rolling;

import java.util.Date;

import com.cloudera.flume.core.Event;

/**
 * A tagger is a mechanism that creates unique names for groups of events and
 * adds annotation data to events. It keeps some state -- the name for most
 * recent group. newTag is called when rotating loggers create a new group (and
 * thus need a new name).
 * 
 */
public interface Tagger {

  public final static String A_TXID = "txid"; // "transaction id"

  String getTag();

  /**
   * Our sinks often rotate and each version needs a separate unique tag/name.
   * This method updates state and creates the next name.
   */
  String newTag();

  Date getDate();

  /**
   * This allows the tagger to add structured data or metadata to an event
   */
  void annotate(Event e);

}
