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
package org.apache.flume.channel.file;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flume.channel.file.proto.ProtosFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

/**
 * Represents a Put on disk
 */
class Put extends TransactionEventRecord {
  private FlumeEvent event;
  // Should we move this to a higher level to not make multiple instances?
  // Doing that might cause performance issues, since access to this would
  // need to be synchronized (the whole reset-update-getValue cycle would
  // need to be).
  private final Checksum checksum = new CRC32();

  @VisibleForTesting
  Put(Long transactionID, Long logWriteOrderID) {
    this(transactionID, logWriteOrderID, null);
  }

  Put(Long transactionID, Long logWriteOrderID, FlumeEvent event) {
    super(transactionID, logWriteOrderID);
    this.event = event;
  }

  FlumeEvent getEvent() {
    return event;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    event = FlumeEvent.from(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    event.write(out);
  }
  @Override
  void writeProtos(OutputStream out) throws IOException {
    ProtosFactory.Put.Builder putBuilder = ProtosFactory.Put.newBuilder();
    ProtosFactory.FlumeEvent.Builder eventBuilder =
        ProtosFactory.FlumeEvent.newBuilder();
    Map<String, String> headers = event.getHeaders();
    ProtosFactory.FlumeEventHeader.Builder headerBuilder =
        ProtosFactory.FlumeEventHeader.newBuilder();
    if(headers != null) {
      for(String key : headers.keySet()) {
        String value = headers.get(key);
        headerBuilder.clear();
        eventBuilder.addHeaders(headerBuilder.setKey(key)
            .setValue(value).build());
      }
    }
    eventBuilder.setBody(ByteString.copyFrom(event.getBody()));
    ProtosFactory.FlumeEvent protoEvent = eventBuilder.build();
    putBuilder.setEvent(protoEvent);
    putBuilder.setChecksum(calculateChecksum(event.getBody()));
    putBuilder.build().writeDelimitedTo(out);
  }
  @Override
  void readProtos(InputStream in) throws IOException,
    CorruptEventException {
    ProtosFactory.Put put = Preconditions.checkNotNull(ProtosFactory.
        Put.parseDelimitedFrom(in), "Put cannot be null");
    Map<String, String> headers = Maps.newHashMap();
    ProtosFactory.FlumeEvent protosEvent = put.getEvent();
    for(ProtosFactory.FlumeEventHeader header : protosEvent.getHeadersList()) {
      headers.put(header.getKey(), header.getValue());
    }
    byte[] eventBody = protosEvent.getBody().toByteArray();

    if (put.hasChecksum()) {
      long eventBodyChecksum = calculateChecksum(eventBody);
      if (eventBodyChecksum != put.getChecksum()) {
        throw new CorruptEventException("Expected checksum for event was " +
          eventBodyChecksum + " but the checksum of the event is " + put.getChecksum());
      }
    }
    // TODO when we remove v2, remove FlumeEvent and use EventBuilder here
    event = new FlumeEvent(headers, eventBody);
  }

  protected long calculateChecksum(byte[] body) {
    checksum.reset();
    checksum.update(body, 0, body.length);
    return checksum.getValue();
  }

  @Override
  public short getRecordType() {
    return Type.PUT.get();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("Put [event=");
    builder.append(event);
    builder.append(", getLogWriteOrderID()=");
    builder.append(getLogWriteOrderID());
    builder.append(", getTransactionID()=");
    builder.append(getTransactionID());
    builder.append("]");
    return builder.toString();
  }

}
