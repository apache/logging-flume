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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import java.util.Collections;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Row;

import java.util.List;

/**
 * For Increment-related unit tests.
 */
class IncrementHBaseSerializer implements HbaseEventSerializer, BatchAware {
  private Event event;
  private byte[] family;
  private int numBatchesStarted = 0;

  @Override public void configure(Context context) { }
  @Override public void configure(ComponentConfiguration conf) { }
  @Override public void close() { }

  @Override
  public void initialize(Event event, byte[] columnFamily) {
    this.event = event;
    this.family = columnFamily;
  }

  // This class only creates Increments.
  @Override
  public List<Row> getActions() {
    return Collections.emptyList();
  }

  // Treat each Event as a String, i,e, "row:qualifier".
  @Override
  public List<Increment> getIncrements() {
    List<Increment> increments = Lists.newArrayList();
    String body = new String(event.getBody(), Charsets.UTF_8);
    String[] pieces = body.split(":");
    String row = pieces[0];
    String qualifier = pieces[1];
    Increment inc = new Increment(row.getBytes(Charsets.UTF_8));
    inc.addColumn(family, qualifier.getBytes(Charsets.UTF_8), 1L);
    increments.add(inc);
    return increments;
  }

  @Override
  public void onBatchStart() {
    numBatchesStarted++;
  }

  @VisibleForTesting
  public int getNumBatchesStarted() {
    return numBatchesStarted;
  }
}
