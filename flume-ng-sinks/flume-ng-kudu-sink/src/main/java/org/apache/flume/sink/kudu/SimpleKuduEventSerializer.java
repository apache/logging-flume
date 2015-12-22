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

package org.apache.flume.sink.kudu;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.conf.ComponentConfiguration;
import org.kududb.client.Insert;
import org.kududb.client.KuduTable;
import org.kududb.client.Operation;
import org.kududb.client.PartialRow;

import java.util.LinkedList;
import java.util.List;

/**
 * A simple serializer that returns puts from an event, by writing the event
 * body into it. The headers are discarded.
 *
 * Optional parameters: <p>
 * <tt>payloadColumn:</tt> Which column to put payload in. If it is null,
 * pCol will be assumed.<p>
 */
public class SimpleKuduEventSerializer implements KuduEventSerializer {
  private byte[] payload;
  private KuduTable table;
  private String payloadColumn;

  public SimpleKuduEventSerializer(){
  }

  @Override
  public void configure(Context context) {
    payloadColumn = context.getString("payloadColumn","pCol");
  }

  @Override
  public void configure(ComponentConfiguration conf) {
  }

  @Override
  public void initialize(Event event, KuduTable table) {
    this.payload = event.getBody();
    this.table = table;
  }

  @Override
  public List<Operation> getOperations() throws FlumeException {
    List<Operation> operations = new LinkedList<Operation>();
    try {
      Insert insert = table.newInsert();
      PartialRow row = insert.getRow();
      row.addBinary(payloadColumn, payload);

      operations.add(insert);
    } catch (Exception e){
      throw new FlumeException("Could not get row key!", e);
    }
    return operations;
  }

  @Override
  public void close() {
  }
}
