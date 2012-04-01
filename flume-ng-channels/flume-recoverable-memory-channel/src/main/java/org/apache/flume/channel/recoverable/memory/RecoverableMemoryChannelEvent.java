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
package org.apache.flume.channel.recoverable.memory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Maps;

class RecoverableMemoryChannelEvent implements Event, Writable {
  Event event;
  long sequenceId;

  // called via reflection
  @SuppressWarnings("unused")
  private RecoverableMemoryChannelEvent() {
    this.event = new SimpleEvent();
  }

  RecoverableMemoryChannelEvent(Event event, long sequenceId) {
    this.event = event;
    this.sequenceId = sequenceId;
  }
  @Override
  public Map<String, String> getHeaders() {
    return event.getHeaders();
  }
  @Override
  public void setHeaders(Map<String, String> headers) {
    event.setHeaders(headers);
  }
  @Override
  public byte[] getBody() {
    return event.getBody();
  }
  @Override
  public void setBody(byte[] body) {
    event.setBody(body);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(sequenceId);
    MapWritable map = toMapWritable(getHeaders());
    map.write(out);
    byte[] body = getBody();
    if(body == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(body.length);
      out.write(body);
    }
  }



  @Override
  public void readFields(DataInput in) throws IOException {
    sequenceId = in.readLong();
    MapWritable map = new MapWritable();
    map.readFields(in);
    setHeaders(fromMapWritable(map));
    byte[] body = null;
    int bodyLength = in.readInt();
    if(bodyLength != -1) {
      body = new byte[bodyLength];
      in.readFully(body);
    }
    setBody(body);
  }
  private MapWritable toMapWritable(Map<String, String> map) {
    MapWritable result = new MapWritable();
    if(map != null) {
      for(Map.Entry<String, String> entry : map.entrySet()) {
        result.put(new Text(entry.getKey()),new Text(entry.getValue()));
      }
    }
    return result;
  }
  private Map<String, String> fromMapWritable(MapWritable map) {
    Map<String, String> result = Maps.newHashMap();
    if(map != null) {
      for(Map.Entry<Writable, Writable> entry : map.entrySet()) {
        result.put(entry.getKey().toString(),entry.getValue().toString());
      }
    }
    return result;
  }
}