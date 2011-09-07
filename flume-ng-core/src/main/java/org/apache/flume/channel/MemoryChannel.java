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

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.Transaction;

public class MemoryChannel implements Channel {

  private BlockingQueue<Event> queue;

  public MemoryChannel() {
    queue = new ArrayBlockingQueue<Event>(50);
  }

  @Override
  public void put(Event event) {
    try {
      queue.put(event);
    } catch (InterruptedException ex) {
      throw new ChannelException("Failed to put(" + event + ")", ex);
    }
  }

  @Override
  public Event take() {
    try {
      return queue.take();
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
}
