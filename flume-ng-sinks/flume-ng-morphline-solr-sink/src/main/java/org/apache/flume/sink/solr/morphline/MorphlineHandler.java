/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.solr.morphline;

import java.io.IOException;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;

/**
 * Interface to load Flume events into Solr
 */
public interface MorphlineHandler extends Configurable {

  /** Begins a transaction */
  public void beginTransaction();

  /** Loads the given event into Solr */
  public void process(Event event);

  /**
   * Sends any outstanding documents to Solr and waits for a positive
   * or negative ack (i.e. exception). Depending on the outcome the caller
   * should then commit or rollback the current flume transaction
   * correspondingly.
   * 
   * @throws IOException
   *           If there is a low-level I/O error.
   */
  public void commitTransaction();

  /**
   * Performs a rollback of all non-committed documents pending.
   * <p>
   * Note that this is not a true rollback as in databases. Content you have previously added to
   * Solr may have already been committed due to autoCommit, buffer full, other client performing a
   * commit etc. So this is only a best-effort rollback.
   * 
   * @throws IOException
   *           If there is a low-level I/O error.
   */
  public void rollbackTransaction();

  /** Releases allocated resources */
  public void stop();


}
