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

/**
 * Represents a Put on disk
 */
class Put extends TransactionEventRecord {
  private FlumeEvent event;

  Put(Long transactionID) {
    super(transactionID);
  }

  Put(Long transactionID, FlumeEvent event) {
    this(transactionID);
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
  public short getRecordType() {
    return Type.PUT.get();
  }
}
