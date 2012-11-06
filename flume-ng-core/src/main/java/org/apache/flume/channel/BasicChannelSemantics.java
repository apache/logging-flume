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

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;

import com.google.common.base.Preconditions;

/**
 * <p>
 * An implementation of basic {@link Channel} semantics, including the
 * implied thread-local semantics of the {@link Transaction} class,
 * which is required to extend {@link BasicTransactionSemantics}.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class BasicChannelSemantics extends AbstractChannel {

  private ThreadLocal<BasicTransactionSemantics> currentTransaction
      = new ThreadLocal<BasicTransactionSemantics>();

  private boolean initialized = false;

  /**
   * <p>
   * Called upon first getTransaction() request, while synchronized on
   * this {@link Channel} instance.  Use this method to delay the
   * initializization resources until just before the first
   * transaction begins.
   * </p>
   */
  protected void initialize() {}

  /**
   * <p>
   * Called to create new {@link Transaction} objects, which must
   * extend {@link BasicTransactionSemantics}.  Each object is used
   * for only one transaction, but is stored in a thread-local and
   * retrieved by <code>getTransaction</code> for the duration of that
   * transaction.
   * </p>
   */
  protected abstract BasicTransactionSemantics createTransaction();

  /**
   * <p>
   * Ensures that a transaction exists for this thread and then
   * delegates the <code>put</code> to the thread's {@link
   * BasicTransactionSemantics} instance.
   * </p>
   */
  @Override
  public void put(Event event) throws ChannelException {
    BasicTransactionSemantics transaction = currentTransaction.get();
    Preconditions.checkState(transaction != null,
        "No transaction exists for this thread");
    transaction.put(event);
  }

  /**
   * <p>
   * Ensures that a transaction exists for this thread and then
   * delegates the <code>take</code> to the thread's {@link
   * BasicTransactionSemantics} instance.
   * </p>
   */
  @Override
  public Event take() throws ChannelException {
    BasicTransactionSemantics transaction = currentTransaction.get();
    Preconditions.checkState(transaction != null,
        "No transaction exists for this thread");
    return transaction.take();
  }

  /**
   * <p>
   * Initializes the channel if it is not already, then checks to see
   * if there is an open transaction for this thread, creating a new
   * one via <code>createTransaction</code> if not.
   * @return the current <code>Transaction</code> object for the
   *     calling thread
   * </p>
   */
  @Override
  public Transaction getTransaction() {

    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          initialize();
          initialized = true;
        }
      }
    }

    BasicTransactionSemantics transaction = currentTransaction.get();
    if (transaction == null || transaction.getState().equals(
            BasicTransactionSemantics.State.CLOSED)) {
      transaction = createTransaction();
      currentTransaction.set(transaction);
    }
    return transaction;
  }
}
