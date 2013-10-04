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
package org.apache.flume.sink.hbase;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * An AsyncHBaseEventSerializer implementation that increments a configured
 * column for the row whose row key is the event's body bytes.
 */
public class IncrementAsyncHBaseSerializer implements AsyncHbaseEventSerializer {
  private byte[] table;
  private byte[] cf;
  private byte[] column;
  private Event currentEvent;
  @Override
  public void initialize(byte[] table, byte[] cf) {
    this.table = table;
    this.cf = cf;
  }

  @Override
  public void setEvent(Event event) {
    this.currentEvent = event;
  }

  @Override
  public List<PutRequest> getActions() {
    return Collections.emptyList();
  }

  @Override
  public List<AtomicIncrementRequest> getIncrements() {
    List<AtomicIncrementRequest> incrs
      = new ArrayList<AtomicIncrementRequest>();
    AtomicIncrementRequest incr = new AtomicIncrementRequest(table,
      currentEvent.getBody(), cf, column, 1);
    incrs.add(incr);
    return incrs;
  }

  @Override
  public void cleanUp() {
  }

  @Override
  public void configure(Context context) {
    column = context.getString("column", "col").getBytes();
  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }
}
