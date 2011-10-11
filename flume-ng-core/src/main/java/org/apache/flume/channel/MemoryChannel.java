/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.flume.channel;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;

import com.google.common.base.Preconditions;

public class MemoryChannel implements Channel, Configurable {

  private static final Integer defaultCapacity = 50;
  private static final Integer defaultKeepAlive = 3;

  private BlockingQueue<Event> queue;
  private Integer keepAlive;

  @Override
  public void configure(Context context) {
    Integer capacity = context.get("capacity", Integer.class);
    keepAlive = context.get("keep-alive", Integer.class);

    if (capacity == null) {
      capacity = defaultCapacity;
    }

    if (keepAlive == null) {
      keepAlive = defaultKeepAlive;
    }

    queue = new ArrayBlockingQueue<Event>(capacity);
  }

  @Override
  public void put(Event event) {
    Preconditions.checkState(queue != null,
        "No queue defined (Did you forget to configure me?");

    try {
      queue.put(event);
    } catch (InterruptedException ex) {
      throw new ChannelException("Failed to put(" + event + ")", ex);
    }
  }

  @Override
  public Event take() {
    Preconditions.checkState(queue != null,
        "No queue defined (Did you forget to configure me?");

    try {
      return queue.poll(keepAlive, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      throw new ChannelException("Failed to take()", ex);
    }
  }

  @Override
  public Transaction getTransaction() {
    return NoOpTransaction.sharedInstance();
  }

  /**
   * <p>
   * A no-op transaction implementation that does nothing at all.
   * </p>
   */
  public static class NoOpTransaction implements Transaction {

    private static NoOpTransaction sharedInstance;

    public static Transaction sharedInstance() {
      if (sharedInstance == null) {
        sharedInstance = new NoOpTransaction();
      }

      return sharedInstance;
    }

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

  @Override
  public void shutdown() {
    // TODO Auto-generated method stub

  }

  @Override
  public String getName() {
    // TODO Auto-generated method stub
    return null;
  }
}
