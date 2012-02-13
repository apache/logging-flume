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

import com.google.common.base.Preconditions;

/**
 * <p>
 * An implementation of basic {@link Transaction} semantics designed
 * to work in concert with {@link BasicChannelSemantics} to simplify
 * creation of robust {@link Channel} implementations.  This class
 * ensures that each transaction implementation method is called only
 * while the transaction is in the correct state for that method, and
 * only by the thread that created the transaction.  Nested calls to
 * <code>begin()</code> and <code>close()</code> are supported as long
 * as they are balanced.
 * </p>
 * <p>
 * Subclasses need only implement <code>doPut</code>,
 * <code>doTake</code>, <code>doCommit</code>, and
 * <code>doRollback</code>, and the developer can rest assured that
 * those methods are called only after transaction state preconditions
 * have been properly met.  <code>doBegin</code> and
 * <code>doClose</code> may also be implemented if there is work to be
 * done at those points.
 * </p>
 * <p>
 * All InterruptedException exceptions thrown from the implementations
 * of the <code>doXXX</code> methods are automatically wrapped to
 * become ChannelExceptions, but only after restoring the interrupted
 * status of the thread so that any subsequent blocking method calls
 * will themselves throw InterruptedException rather than blocking.
 * The exception to this rule is <code>doTake</code>, which simply
 * returns null instead of wrapping and propagating the
 * InterruptedException, though it still first restores the
 * interrupted status of the thread.
 * </p>
 */
public abstract class BasicTransactionSemantics implements Transaction {

  private State state;
  private long initialThreadId;

  protected void doBegin() throws InterruptedException {}
  protected abstract void doPut(Event event) throws InterruptedException;
  protected abstract Event doTake() throws InterruptedException;
  protected abstract void doCommit() throws InterruptedException;
  protected abstract void doRollback() throws InterruptedException;
  protected void doClose() {}

  protected BasicTransactionSemantics() {
    state = State.NEW;
    initialThreadId = Thread.currentThread().getId();
  }

  /**
   * <p>
   * The method to which {@link BasicChannelSemantics} delegates calls
   * to <code>put</code>.
   * </p>
   */
  protected void put(Event event) {
    Preconditions.checkState(Thread.currentThread().getId() == initialThreadId,
        "put() called from different thread than getTransaction()!");
    Preconditions.checkState(state.equals(State.OPEN),
        "put() called when transaction is %s!", state);
    Preconditions.checkArgument(event != null,
        "put() called with null event!");

    try {
      doPut(event);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ChannelException(e.toString(), e);
    }
  }

  /**
   * <p>
   * The method to which {@link BasicChannelSemantics} delegates calls
   * to <code>take</code>.
   * </p>
   */
  protected Event take() {
    Preconditions.checkState(Thread.currentThread().getId() == initialThreadId,
        "take() called from different thread than getTransaction()!");
    Preconditions.checkState(state.equals(State.OPEN),
        "take() called when transaction is %s!", state);

    try {
      return doTake();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  /**
   * @return the current state of the transaction
   */
  protected State getState() {
    return state;
  }

  @Override
  public void begin() {
    Preconditions.checkState(Thread.currentThread().getId() == initialThreadId,
        "begin() called from different thread than getTransaction()!");
    Preconditions.checkState(state.equals(State.NEW),
        "begin() called when transaction is " + state + "!");

    try {
      doBegin();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ChannelException(e.toString(), e);
    }
    state = State.OPEN;
  }

  @Override
  public void commit() {
    Preconditions.checkState(Thread.currentThread().getId() == initialThreadId,
        "commit() called from different thread than getTransaction()!");
    Preconditions.checkState(state.equals(State.OPEN),
        "commit() called when transaction is %s!", state);

    try {
      doCommit();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ChannelException(e.toString(), e);
    }
    state = State.COMPLETED;
  }

  @Override
  public void rollback() {
    Preconditions.checkState(Thread.currentThread().getId() == initialThreadId,
        "rollback() called from different thread than getTransaction()!");
    Preconditions.checkState(state.equals(State.OPEN),
        "rollback() called when transaction is %s!", state);

    state = State.COMPLETED;
    try {
      doRollback();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ChannelException(e.toString(), e);
    }
  }

  @Override
  public void close() {
    Preconditions.checkState(Thread.currentThread().getId() == initialThreadId,
        "close() called from different thread than getTransaction()!");
    Preconditions.checkState(
            state.equals(State.NEW) || state.equals(State.COMPLETED),
            "close() called when transaction is %s"
            + " - you must either commit or rollback first", state);

    state = State.CLOSED;
    doClose();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("BasicTransactionSemantics: {");
    builder.append(" state:").append(state);
    builder.append(" initialThreadId:").append(initialThreadId);
    builder.append(" }");
    return builder.toString();
  }

  /**
   * <p>
   * The state of the {@link Transaction} to which it belongs.
   * </p>
   * <dl>
   * <dt>NEW</dt>
   * <dd>A newly created transaction that has not yet begun.</dd>
   * <dt>OPEN</dt>
   * <dd>A transaction that is open. It is permissible to commit or rollback.
   * </dd>
   * <dt>COMPLETED</dt>
   * <dd>This transaction has been committed or rolled back. It is illegal to
   * perform any further operations beyond closing it.</dd>
   * <dt>CLOSED</dt>
   * <dd>A closed transaction. No further operations are permitted.</dd>
   */
  protected static enum State {
    NEW, OPEN, COMPLETED, CLOSED
  }
}
