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
package org.apache.flume.channel.jdbc.impl;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;

import javax.sql.DataSource;

import org.apache.flume.Transaction;
import org.apache.flume.channel.jdbc.JdbcChannelException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcTransactionImpl implements Transaction {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(JdbcTransactionImpl.class);

  private final DataSource dataSource;
  private final JdbcChannelProviderImpl providerImpl;
  private Connection connection;
  private JdbcTransactionFactory txFactory;
  private boolean active = true;

  /** Reference count used to do the eventual commit.*/
  private int count = 0;

  /** Number of events successfully removed from the channel. */
  private int removedEventCount = 0;

  /** Number of events persisted to the channel. */
  private int persistedEventCount = 0;

  /** Flag that indicates if the transaction must be rolled back. */
  private boolean rollback = false;

  protected JdbcTransactionImpl(DataSource dataSource,
      JdbcTransactionFactory factory, JdbcChannelProviderImpl provider) {
    this.dataSource = dataSource;
    txFactory = factory;
    providerImpl = provider;
  }

  @Override
  public void begin() {
    if (!active) {
      throw new JdbcChannelException("Inactive transaction");
    }
    if (count == 0) {
      // Lease a connection now
      try {
        connection = dataSource.getConnection();
      } catch (SQLException ex) {
        throw new JdbcChannelException("Unable to lease connection", ex);
      }
      // Clear any prior warnings on the connection
      try {
        connection.clearWarnings();
      } catch (SQLException ex) {
        LOGGER.error("Error while clearing warnings: " + ex.getErrorCode(), ex);
      }
    }
    count++;
    LOGGER.trace("Tx count-begin: " + count + ", rollback: " + rollback);
  }

  @Override
  public void commit() {
    if (!active) {
      throw new JdbcChannelException("Inactive transaction");
    }
    if (rollback) {
      throw new JdbcChannelException(
          "Cannot commit transaction marked for rollback");
    }
    LOGGER.trace("Tx count-commit: " + count + ", rollback: " + rollback);
  }

  @Override
  public void rollback() {
    if (!active) {
      throw new JdbcChannelException("Inactive transaction");
    }
    LOGGER.warn("Marking transaction for rollback");
    rollback = true;
    LOGGER.trace("Tx count-rollback: " + count + ", rollback: " + rollback);
  }

  @Override
  public void close() {
    if (!active) {
      throw new JdbcChannelException("Inactive transaction");
    }
    count--;
    LOGGER.debug("Tx count-close: " + count + ", rollback: " + rollback);
    if (count == 0) {
      active = false;
      try {
        if (rollback) {
          LOGGER.info("Attempting transaction roll-back");
          connection.rollback();
        } else {
          LOGGER.debug("Attempting transaction commit");
          connection.commit();

          // Commit successful. Update provider channel size
          providerImpl.updateCurrentChannelSize(this.persistedEventCount
              - this.removedEventCount);

          this.persistedEventCount = 0;
          this.removedEventCount = 0;

        }
      } catch (SQLException ex) {
        throw new JdbcChannelException("Unable to finalize transaction", ex);
      } finally {
        if (connection != null) {
          // Log Warnings
          try {
            SQLWarning warning = connection.getWarnings();
            if (warning != null) {
              StringBuilder sb = new StringBuilder("Connection warnigns: ");
              boolean first = true;
              while (warning != null) {
                if (first) {
                  first = false;
                } else {
                  sb.append("; ");
                }
                sb.append("[").append(warning.getErrorCode()).append("] ");
                sb.append(warning.getMessage());
              }
              LOGGER.warn(sb.toString());
            }
          } catch (SQLException ex) {
            LOGGER.error("Error while retrieving warnigns: "
                                + ex.getErrorCode(), ex);
          }

          // Close Connection
          try {
            connection.close();
          } catch (SQLException ex) {
            LOGGER.error(
                "Unable to close connection: " + ex.getErrorCode(), ex);
          }
        }

        // Clean up thread local
        txFactory.remove();

        // Destroy local state
        connection = null;
        txFactory = null;
      }
    }
  }

  protected Connection getConnection() {
    if (!active) {
      throw new JdbcChannelException("Inactive transaction");
    }
    return connection;
  }

  protected void incrementRemovedEventCount() {
    removedEventCount++;
  }

  protected void incrementPersistedEventCount() {
    persistedEventCount++;
  }
}
