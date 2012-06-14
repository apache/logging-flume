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

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.SimpleHbaseEventSerializer.KeyType;

import com.google.common.base.Charsets;

public class SimpleAsyncHbaseEventSerializer implements AsyncHbaseEventSerializer {
  private byte[] table;
  private byte[] cf;
  private byte[] payload;
  private byte[] payloadColumn;
  private byte[] incrementColumn;
  private String rowPrefix;
  private byte[] incrementRow;
  private KeyType keyType;

  @Override
  public void initialize(byte[] table, byte[] cf) {
    this.table = table;
    this.cf = cf;
  }

  @Override
  public List<PutRequest> getActions() {
    List<PutRequest> actions = new ArrayList<PutRequest>();
    if(payloadColumn != null){
      byte[] rowKey;
      try {
        if (keyType == KeyType.TS) {
          rowKey = SimpleRowKeyGenerator.getNanoTimestampKey(rowPrefix);
        } else if(keyType == KeyType.RANDOM) {
          rowKey = SimpleRowKeyGenerator.getRandomKey(rowPrefix);
        } else if(keyType == KeyType.TSNANO) {
          rowKey = SimpleRowKeyGenerator.getNanoTimestampKey(rowPrefix);
        } else {
          rowKey = SimpleRowKeyGenerator.getUUIDKey(rowPrefix);
        }
        PutRequest putRequest =  new PutRequest(table, rowKey, cf,
            payloadColumn, payload);
        actions.add(putRequest);
      } catch (Exception e){
        throw new FlumeException("Could not get row key!", e);
      }
    }
    return actions;
  }

  public List<AtomicIncrementRequest> getIncrements(){
    List<AtomicIncrementRequest> actions = new
        ArrayList<AtomicIncrementRequest>();
    if(incrementColumn != null) {
      AtomicIncrementRequest inc = new AtomicIncrementRequest(table,
          incrementRow, cf, incrementColumn);
      actions.add(inc);
    }
    return actions;
  }

  @Override
  public void cleanUp() {
    // TODO Auto-generated method stub

  }

  @Override
  public void configure(Context context) {
    String pCol = context.getString("payloadColumn");
    String iCol = context.getString("incrementColumn");
    rowPrefix = context.getString("rowPrefix", "default");
    String suffix = context.getString("suffix", "uuid");
    if(pCol != null && !pCol.isEmpty()) {
      if(suffix.equals("timestamp")){
        keyType = KeyType.TS;
      } else if (suffix.equals("random")) {
        keyType = KeyType.RANDOM;
      } else if(suffix.equals("nano")){
        keyType = KeyType.TSNANO;
      } else {
        keyType = KeyType.UUID;
      }
      payloadColumn = pCol.getBytes(Charsets.UTF_8);
    }
    if(iCol != null && !iCol.isEmpty()) {
      incrementColumn = iCol.getBytes(Charsets.UTF_8);
    }
    incrementRow =
        context.getString("incrementRow", "incRow").getBytes(Charsets.UTF_8);
  }

  @Override
  public void setEvent(Event event) {
    this.payload = event.getBody();
  }

  @Override
  public void configure(ComponentConfiguration conf) {
    // TODO Auto-generated method stub
  }

}
