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
package com.cloudera.flume.handlers.thrift;

import java.io.IOException;

import org.apache.thrift.TException;

import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.hdfs.WriteableEvent;
import com.cloudera.flume.handlers.thrift.ThriftFlumeEventServer.Iface;
import com.google.common.base.Preconditions;

class ThriftFlumeEventServerImpl implements Iface {

  EventSink sink;

  ThriftFlumeEventServerImpl(EventSink sink) {
    this.sink = sink;
  }

  @Override
  public void append(ThriftFlumeEvent evt) throws TException {
    Preconditions.checkState(sink != null);
    Preconditions.checkNotNull(evt);
    try {
      sink.append(new ThriftEventAdaptor(evt));
    } catch (IOException e) {
      e.printStackTrace();
      throw new TException("Caught IO exception " + e);
    }
  }

  @Override
  public void close() throws TException {
    try {
      sink.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new TException("Caught IO exception " + e);
    }
  }

  @Override
  public void rawAppend(RawEvent evt) throws TException {
    try {
      WriteableEvent e = WriteableEvent.create(evt.getRaw());
      sink.append(e);
    } catch (IOException e) {
      e.printStackTrace();
      throw new TException("Caught IO exception " + e);
    }

  }

  @Override
  public EventStatus ackedAppend(ThriftFlumeEvent evt) throws TException {
    Preconditions.checkState(sink != null);
    Preconditions.checkNotNull(evt);
    try {
      sink.append(new ThriftEventAdaptor(evt));
      return EventStatus.ACK;
    } catch (IOException e) {
      e.printStackTrace();
      return EventStatus.ERR;
    }
  }

}
