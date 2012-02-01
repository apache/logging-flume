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
package org.apache.flume.channel;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.Transaction;

public class MockChannel extends AbstractChannel {

  private List<Event> events = new ArrayList<Event>();

  public static Channel createMockChannel(String name) {
    Channel ch = new MockChannel();
    ch.setName(name);
    return ch;
  }

  @Override
  public void put(Event event) throws ChannelException {
    events.add(event);
  }

  @Override
  public Event take() throws ChannelException {
    return (events.size() > 0) ? events.get(0) : null;
  }

  @Override
  public Transaction getTransaction() {
    return new MockTransaction();
  }

  private static class MockTransaction implements Transaction {

    @Override
    public void begin() {
    }

    @Override
    public void commit() {
    }

    @Override
    public void rollback() {
    }

    @Override
    public void close() {
    }
  }

}
