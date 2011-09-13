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
package com.cloudera.flume.log4j.appender;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.HttpTransceiver;
import org.apache.avro.ipc.Transceiver;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

import com.cloudera.flume.handlers.avro.AvroEventConvertUtil;
import com.cloudera.flume.handlers.avro.FlumeEventAvroServer;
import com.cloudera.flume.handlers.log4j.Log4JEventAdaptor;

/**
 * <p>
 * A log4j appender implementation that logs directly to Flume's Avro source
 * without writing to disk.
 * </p>
 * <p>
 * This appender purposefully does not buffer any events. While this does hurt
 * performance, it ensures Flume's reliability settings and semantics are
 * preserved. In other words, we want to ensure that after the log4j method call
 * returns, data is safely on disk in the case of end to end reliability.
 * </p>
 * <p>
 * The only parameter that absolutely must be set is the port on which the Flume
 * avroSource is listening. The appender assumes the Flume agent is running
 * locally and that we can communicate via the hostname
 * <q>localhost.</q> Users can also control the number of times to attempt
 * reconnection before a logging call fails.
 * </p>
 * <p>
 * Parameters:
 * <dl>
 * <dt>hostname</dt>
 * <dd>The hostname or IP where we should attempt to send events. (default:
 * localhost)</dd>
 * <dt>port</dt>
 * <dd>The port on which Flume's avroSource is configured to listen. (required)</dd>
 * <dt>reconnectAttempts</dt>
 * <dd>The maximum number of times we should attempt to connect to the
 * avroSource before throwing an exception. A setting of 0 (zero) means to try
 * forever. (default: 10)</dd>
 * </dl>
 * </p>
 * <p>
 * Example log4j.properties
 * 
 * <pre>
 *  log4j.debug = true
 *  log4j.rootLogger = INFO, flume
 *  
 *  log4j.appender.flume = com.cloudera.flume.log4j.appender.FlumeLog4jAvroAppender
 *  log4j.appender.flume.layout = org.apache.log4j.TTCCLayout
 *  log4j.appender.flume.port = 12345
 *  log4j.appender.flume.hostname = localhost
 *  log4j.appender.flume.reconnectAttempts = 10
 * </pre>
 * 
 * Example Flume configuration
 * 
 * <pre>
 * my-app : avroSource(12345) | agentE2ESink("my-app-col", 12346)
 * my-app-col : collectorSource(12346) | collectorSink("hdfs://...", "my-app-")
 * </pre>
 * 
 * </p>
 * <p>
 * Note that this class does <b>not</b> attempt to be thread safe but inherits
 * from {@link AppenderSkeleton} which handles proper synchronization during
 * logging operations. This should only be important to those accessing the
 * class directly or writing tests.
 * </p>
 */
public class FlumeLog4jAvroAppender extends AppenderSkeleton {

  private static final int defaultReconnectAttempts = 10;
  private static final String defaultHostname = "localhost";

  private FlumeEventAvroServer client;

  /*
   * NB: Log4j uses direct field access for configuration and thus great care
   * should be taken when adding, removing, or renaming fields. These are part
   * of the user visible configuration interface.
   */
  protected String hostname;
  protected int port;
  protected int reconnectAttempts;

  public FlumeLog4jAvroAppender() {
    super();

    reconnectAttempts = defaultReconnectAttempts;
    hostname = defaultHostname;
  }

  private void connect() {
    int attempt = 0;

    LogLog.debug("attempting to create an Avro connection");

    while (reconnectAttempts == 0 || attempt <= reconnectAttempts) {
      LogLog.debug("reconnectAttempts allow:" + reconnectAttempts + " attempt:"
          + attempt);

      client = attemptConnection();

      if (client == null) {
        LogLog.debug("connection failed - sleeping");

        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        attempt++;
      } else {
        // Got a connection.
        return;
      }
    }

    LogLog.warn("Exhausted Avro server connection attempts (reconnectAttempts:"
        + reconnectAttempts + " attempt:" + attempt
        + "). This appender won't function.");

  }

  private FlumeEventAvroServer attemptConnection() {
    LogLog.debug("connecting to Avro server hostname:" + hostname + " port:"
        + port);

    URL url = null;

    try {
      url = new URL("http", hostname, port, "/");
    } catch (MalformedURLException e) {
      LogLog.warn("Unable to create a well-formed URL with hostname:"
          + hostname + " port:" + port, e);
    }

    LogLog.debug("using url:" + url);

    Transceiver transciever = new HttpTransceiver(url);
    FlumeEventAvroServer client = null;

    try {
      client = SpecificRequestor.getClient(FlumeEventAvroServer.class,
          transciever);
    } catch (IOException e) {
      LogLog.warn("Unable to create Avro client", e);
    }

    return client;
  }

  @Override
  public void close() {
    client = null;
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }

  @Override
  protected void append(LoggingEvent event) {
    if (client == null) {
      connect();
    }

    int attempt = 1;

    while (reconnectAttempts == 0 || attempt <= reconnectAttempts) {
      try {
        /*
         * This is not the nicest way to do this. Ideally we'd skip the
         * intermediate object and go from the log4j event directly to the
         * AvroFlumeEvent.
         */
        client.append(AvroEventConvertUtil.toAvroEvent(new Log4JEventAdaptor(event)));

        break;
      } catch (UndeclaredThrowableException e) {
        /*
         * This is yucky. We want to give client.append() $reconnectAttempts
         * tries to succeed. If it causes an undeclared exception, we want to
         * check if it's a ConnectException. If so, we want to rethrow it if
         * we're out of attempts. Otherwise, we want to attempt to reconnect and
         * try again. It would be nice to express this logic without repeating
         * the loop condition and without doing things like extending the
         * lifetime of the exception out of the catch block.
         */
        if (reconnectAttempts > 0 && attempt >= reconnectAttempts) {
          throw e;
        }

        Throwable cause = e.getCause();

        if (cause instanceof AvroRemoteException) {
          cause = cause.getCause();
        }

        /*
         * We're only interested in attempting to recover from connection
         * exceptions right now.
         */
        if (cause instanceof ConnectException) {
          LogLog.warn("Failed to communicate with server. reconnectAttempts:"
              + reconnectAttempts + " attempt:" + attempt, cause);

          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            Thread.currentThread().interrupt();
          }

          client = null;
          connect();
        } else {
          throw e;
        }
      }

      attempt++;
    }
  }

  public FlumeEventAvroServer getClient() {
    return client;
  }

  public void setClient(FlumeEventAvroServer client) {
    this.client = client;
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getReconnectAttempts() {
    return reconnectAttempts;
  }

  public void setReconnectAttempts(int reconnectAttempts) {
    this.reconnectAttempts = reconnectAttempts;
  }

}
