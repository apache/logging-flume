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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A collection of utilities for interacting with {@link Channel}
 * objects.  Use of these utilities prevents error-prone replication
 * of required transaction usage semantics, and allows for more
 * concise code.
 * </p>
 * <p>
 * However, as a side-effect of its generality, and in particular of
 * its use of {@link Callable}, any checked exceptions thrown by
 * user-created transactors will be silently wrapped with {@link
 * ChannelException} before being propagated.  Only direct use of
 * {@link #transact(Channel,Callable)} suffers from this issue, even
 * though all other methods are based upon it, because none of the
 * other methods are capable of producing or allowing checked
 * exceptions in the first place.
 * </p>
 */
public class ChannelUtils {

  private static final Logger logger = LoggerFactory
      .getLogger(ChannelUtils.class);

  /**
   * <p>
   * A convenience method for single-event <code>put</code> transactions.
   * </p>
   * @see #transact(Channel,Callable)
   */
  public static void put(final Channel channel, final Event event)
      throws ChannelException {
    transact(channel, new Runnable() {
        @Override
        public void run() {
          channel.put(event);
        }
      });
  }

  /**
   * <p>
   * A convenience method for multiple-event <code>put</code> transactions.
   * </p>
   * @see #transact(Channel,Callable)
   */
  public static void put(final Channel channel, final Collection<Event> events)
      throws ChannelException {
    transact(channel, new Runnable() {
        @Override
        public void run() {
          for (Event event : events) {
            channel.put(event);
          }
        }
      });
  }

  /**
   * <p>
   * A convenience method for single-event <code>take</code> transactions.
   * </p>
   * @return a single event, or null if the channel has none available
   * @see #transact(Channel,Callable)
   */
  public static Event take(final Channel channel)
      throws ChannelException {
    return transact(channel, new Callable<Event>() {
        @Override
        public Event call() {
          return channel.take();
        }
      });
  }

  /**
   * <p>
   * A convenience method for multiple-event <code>take</code> transactions.
   * </p>
   * @return a list of at most <code>max</code> events
   * @see #transact(Channel,Callable)
   */
  public static List<Event> take(final Channel channel, final int max)
      throws ChannelException {
    return transact(channel, new Callable<List<Event>>() {
        @Override
        public List<Event> call() {
          List<Event> events = new ArrayList<Event>(max);
          while (events.size() < max) {
            Event event = channel.take();
            if (event == null) {
              break;
            }
            events.add(event);
          }
          return events;
        }
      });
  }

  /**
   * <p>
   * A convenience method for transactions that don't require a return
   * value.  Simply wraps the <code>transactor</code> using {@link
   * Executors#callable} and passes that to {@link
   * #transact(Channel,Callable)}.
   * </p>
   * @see #transact(Channel,Callable)
   * @see Executors#callable(Runnable)
   */
  public static void transact(Channel channel, Runnable transactor)
      throws ChannelException {
    transact(channel, Executors.callable(transactor));
  }

  /**
   * <p>
   * A general optimistic implementation of {@link Transaction} client
   * semantics.  It gets a new transaction object from the
   * <code>channel</code>, calls <code>begin()</code> on it, and then
   * invokes the supplied <code>transactor</code> object.  If an
   * exception is thrown, then the transaction is rolled back;
   * otherwise the transaction is committed and the value returned by
   * the <code>transactor</code> is returned.  In either case, the
   * transaction is closed before the function exits.  All secondary
   * exceptions (i.e. those thrown by
   * <code>Transaction.rollback()</code> or
   * <code>Transaction.close()</code> while recovering from an earlier
   * exception) are logged, allowing the original exception to be
   * propagated instead.
   * </p>
   * <p>
   * This implementation is optimistic in that it expects transaction
   * rollback to be infrequent: it will rollback a transaction only
   * when the supplied <code>transactor</code> throws an exception,
   * and exceptions are a fairly heavyweight mechanism for handling
   * frequently-occurring events.
   * </p>
   * @return the value returned by <code>transactor.call()</code>
   */
  public static <T> T transact(Channel channel, Callable<T> transactor)
      throws ChannelException {
    Transaction transaction = channel.getTransaction();
    boolean committed = false;
    boolean interrupted = false;
    try {
      transaction.begin();
      T value = transactor.call();
      transaction.commit();
      committed = true;
      return value;
    } catch (Throwable e) {
      interrupted = Thread.currentThread().isInterrupted();
      try {
        transaction.rollback();
      } catch (Throwable e2) {
        logger.error("Failed to roll back transaction, exception follows:", e2);
      }
      if (e instanceof InterruptedException) {
        interrupted = true;
      } else if (e instanceof Error) {
        throw (Error) e;
      } else if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new ChannelException(e);
    } finally {
      interrupted = interrupted || Thread.currentThread().isInterrupted();
      try {
        transaction.close();
      } catch (Throwable e) {
        if (committed) {
          if (e instanceof Error) {
            throw (Error) e;
          } else if (e instanceof RuntimeException) {
            throw (RuntimeException) e;
          } else {
            throw new ChannelException(e);
          }
        } else {
          logger.error(
              "Failed to close transaction after error, exception follows:", e);
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /** Disallows instantiation */
  private ChannelUtils() {}
}
