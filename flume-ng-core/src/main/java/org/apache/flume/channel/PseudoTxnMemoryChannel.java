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

import com.google.common.base.Preconditions;
import org.apache.flume.instrumentation.ChannelCounter;

/**
 * <p>
 * A capacity-capped {@link Channel} implementation that supports in-memory
 * buffering and delivery of events.
 * </p>
 * <p>
 * This channel is appropriate for
 * <q>best effort</q> delivery of events where high throughput is favored over
 * data durability. To be clear, <b>this channel offers absolutely no guarantee
 * of event delivery</b> in the face of (any) component failure.
 * </p>
 * <p>
 * TODO: Discuss guarantees, corner cases re: potential data loss (e.g. consumer
 * begins a tx, takes events, and gets SIGKILL before rollback).
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <table>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * <th>Unit / Type</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td><tt>capacity</tt></td>
 * <td>The in-memory capacity of this channel. Store up to <tt>capacity</tt>
 * events before refusing new events.</td>
 * <td>events / int</td>
 * <td>50</td>
 * </tr>
 * <tr>
 * <td><tt>keep-alive</tt></td>
 * <td>The amount of time (seconds) to wait for an event before returning
 * <tt>null</tt> on {@link #take()}.</td>
 * <td>seconds / int</td>
 * <td>3</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class PseudoTxnMemoryChannel extends AbstractChannel {

  private static final Integer defaultCapacity = 50;
  private static final Integer defaultKeepAlive = 3;

  private BlockingQueue<Event> queue;
  private Integer keepAlive;
  private ChannelCounter channelCounter;

  @Override
  public void configure(Context context) {
    Integer capacity = context.getInteger("capacity");
    keepAlive = context.getInteger("keep-alive");

    if (capacity == null) {
      capacity = defaultCapacity;
    }

    if (keepAlive == null) {
      keepAlive = defaultKeepAlive;
    }

    queue = new ArrayBlockingQueue<Event>(capacity);
    if(channelCounter == null) {
      channelCounter = new ChannelCounter(getName());
    }
  }

  @Override
  public void start(){
    channelCounter.start();
    channelCounter.setChannelSize(queue.size());
    channelCounter.setChannelSize(
            Long.valueOf(queue.size() + queue.remainingCapacity()));
    super.start();
  }

  @Override
  public void stop(){
    channelCounter.setChannelSize(queue.size());
    channelCounter.stop();
    super.stop();
  }

  @Override
  public void put(Event event) {
    Preconditions.checkState(queue != null,
        "No queue defined (Did you forget to configure me?");
    channelCounter.incrementEventPutAttemptCount();
    try {
      queue.put(event);
    } catch (InterruptedException ex) {
      throw new ChannelException("Failed to put(" + event + ")", ex);
    }
    channelCounter.addToEventPutSuccessCount(1);
    channelCounter.setChannelSize(queue.size());
  }

  @Override
  public Event take() {
    Preconditions.checkState(queue != null,
        "No queue defined (Did you forget to configure me?");
    channelCounter.incrementEventTakeAttemptCount();
    try {
      Event e = queue.poll(keepAlive, TimeUnit.SECONDS);
      channelCounter.addToEventTakeSuccessCount(1);
      channelCounter.setChannelSize(queue.size());
      return e;
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
