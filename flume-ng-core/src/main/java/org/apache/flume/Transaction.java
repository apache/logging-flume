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
package org.apache.flume;

import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;

/**
 * <p>Provides the transaction boundary while accessing a channel.</p>
 * <p>A <tt>Transaction</tt> instance is used to encompass channel access
 * via the following idiom:</p>
 * <pre><code>
 * Channel ch = ...
 * Transaction tx = ch.getTransaction();
 * try {
 *   tx.begin();
 *   ...
 *   // ch.put(event) or ch.take()
 *   ...
 *   tx.commit();
 * } catch (ChannelException ex) {
 *   tx.rollback();
 *   ...
 * } finally {
 *   tx.close();
 * }
 * </code></pre>
 * <p>Depending upon the implementation of the channel, the transaction
 * semantics may be strong, or best-effort only.</p>
 *
 * <p>
 * Transactions must be thread safe. To provide  a guarantee of thread safe
 * access to Transactions, see {@link BasicChannelSemantics} and
 * {@link  BasicTransactionSemantics}.
 *
 * @see org.apache.flume.Channel
 */
public interface Transaction {

public enum TransactionState {Started, Committed, RolledBack, Closed };
  /**
   * <p>Starts a transaction boundary for the current channel operation. If a
   * transaction is already in progress, this method will join that transaction
   * using reference counting.</p>
   * <p><strong>Note</strong>: For every invocation of this method there must
   * be a corresponding invocation of {@linkplain #close()} method. Failure
   * to ensure this can lead to dangling transactions and unpredictable results.
   * </p>
   */
  public void begin();

  /**
   * Indicates that the transaction can be successfully committed. It is
   * required that a transaction be in progress when this method is invoked.
   */
  public void commit();

  /**
   * Indicates that the transaction can must be aborted. It is
   * required that a transaction be in progress when this method is invoked.
   */
  public void rollback();

  /**
   * <p>Ends a transaction boundary for the current channel operation. If a
   * transaction is already in progress, this method will join that transaction
   * using reference counting. The transaction is completed only if there
   * are no more references left for this transaction.</p>
   * <p><strong>Note</strong>: For every invocation of this method there must
   * be a corresponding invocation of {@linkplain #begin()} method. Failure
   * to ensure this can lead to dangling transactions and unpredictable results.
   * </p>
   */
  public void close();
}
