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
package org.apache.flume.sink.http;

import com.google.common.collect.ImmutableMap;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of an HTTP sink. Events are POSTed to an HTTP / HTTPS
 * endpoint. The error handling behaviour is configurable, and can respond
 * differently depending on the response status returned by the endpoint.
 *
 * Rollback of the Flume transaction, and backoff can be specified globally,
 * then overridden for ranges (or individual) status codes.
 */
public class HttpSink extends AbstractSink implements Configurable {

  /** Class logger. */
  private static final Logger LOG = Logger.getLogger(HttpSink.class);

  /** Lowest valid HTTP status code. */
  private static final int HTTP_STATUS_CONTINUE = 100;

  /** Default setting for the connection timeout when calling endpoint. */
  private static final int DEFAULT_CONNECT_TIMEOUT = 5000;

  /** Default setting for the request timeout when calling endpoint. */
  private static final int DEFAULT_REQUEST_TIMEOUT = 5000;

  /** Default setting for the HTTP content type header. */
  private static final String DEFAULT_CONTENT_TYPE = "text/plain";

  /** Default setting for the HTTP accept header. */
  private static final String DEFAULT_ACCEPT_HEADER = "text/plain";

  /** Endpoint URL to POST events to. */
  private URL endpointUrl;

  /** Counter used to monitor event throughput. */
  private SinkCounter sinkCounter;

  /** Actual connection timeout value in use. */
  private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;

  /** Actual request timeout value in use. */
  private int requestTimeout = DEFAULT_REQUEST_TIMEOUT;

  /** Actual content type header value in use. */
  private String contentTypeHeader = DEFAULT_CONTENT_TYPE;

  /** Actual accept header value in use. */
  private String acceptHeader = DEFAULT_ACCEPT_HEADER;

  /** Backoff value to use if a specific override is not defined. */
  private boolean defaultBackoff;

  /** Rollback value to use if a specific override is not defined. */
  private boolean defaultRollback;

  /** Increment metrics value to use if a specific override is not defined. */
  private boolean defaultIncrementMetrics;

  /**
   * Holds all overrides for backoff. The key is a string of the format "500" or
   * "5XX", and the value is the backoff value to use for the individual code,
   * or code range.
   */
  private HashMap<String, Boolean> backoffOverrides = new HashMap<>();

  /**
   * Holds all overrides for rollback. The key is a string of the format "500"
   * or "5XX", and the value is the rollback value to use for the individual
   * code, or code range.
   */
  private HashMap<String, Boolean> rollbackOverrides = new HashMap<>();

  /**
   * Holds all overrides for increment metrics. The key is a string of the
   * format "500" or "5XX", and the value is the increment metrics value to use
   * for the individual code, or code range.
   */
  private HashMap<String, Boolean> incrementMetricsOverrides = new HashMap<>();

  /** Used to create HTTP connections to the endpoint. */
  private ConnectionBuilder connectionBuilder;

  @Override
  public final void configure(final Context context) {
    String configuredEndpoint = context.getString("endpoint", "");
    LOG.info("Read endpoint URL from configuration : " + configuredEndpoint);

    try {
      endpointUrl = new URL(configuredEndpoint);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Endpoint URL invalid", e);
    }

    connectTimeout = context.getInteger("connectTimeout",
        DEFAULT_CONNECT_TIMEOUT);

    if (connectTimeout <= 0) {
      throw new IllegalArgumentException(
          "Connect timeout must be a non-zero and positive");
    }
    LOG.info("Using connect timeout : " + connectTimeout);

    requestTimeout = context.getInteger("requestTimeout",
        DEFAULT_REQUEST_TIMEOUT);

    if (requestTimeout <= 0) {
      throw new IllegalArgumentException(
          "Request timeout must be a non-zero and positive");
    }
    LOG.info("Using request timeout : " + requestTimeout);

    acceptHeader = context.getString("acceptHeader", DEFAULT_ACCEPT_HEADER);
    LOG.info("Using Accept header value : " + acceptHeader);

    contentTypeHeader = context.getString("contentTypeHeader",
        DEFAULT_CONTENT_TYPE);
    LOG.info("Using Content-Type header value : " + contentTypeHeader);

    defaultBackoff = context.getBoolean("defaultBackoff", true);
    LOG.info("Channel backoff by default is " + defaultBackoff);

    defaultRollback = context.getBoolean("defaultRollback", true);
    LOG.info("Transaction rollback by default is " + defaultRollback);

    defaultIncrementMetrics = context.getBoolean("defaultIncrementMetrics",
        false);
    LOG.info("Incrementing metrics by default is " + defaultIncrementMetrics);

    parseConfigOverrides("backoff", context, backoffOverrides);
    parseConfigOverrides("rollback", context, rollbackOverrides);
    parseConfigOverrides("incrementMetrics", context,
        incrementMetricsOverrides);

    if (this.sinkCounter == null) {
      this.sinkCounter = new SinkCounter(this.getName());
    }

    connectionBuilder = new ConnectionBuilder();
  }

  @Override
  public final void start() {
    LOG.info("Starting HttpSink");
    sinkCounter.start();
  }

  @Override
  public final void stop() {
    LOG.info("Stopping HttpSink");
    sinkCounter.stop();
  }

  @Override
  public final Status process() throws EventDeliveryException {
    Status status = null;
    OutputStream outputStream = null;

    Channel ch = getChannel();
    Transaction txn = ch.getTransaction();
    txn.begin();

    try {
      Event event = ch.take();

      byte[] eventBody = null;
      if (event != null) {
        eventBody = event.getBody();
      }

      if (eventBody != null && eventBody.length > 0) {
        sinkCounter.incrementEventDrainAttemptCount();
        LOG.debug("Sending request : " + new String(event.getBody()));

        try {
          HttpURLConnection connection = connectionBuilder.getConnection();

          outputStream = connection.getOutputStream();
          outputStream.write(eventBody);
          outputStream.flush();
          outputStream.close();

          int httpStatusCode = connection.getResponseCode();
          LOG.debug("Got status code : " + httpStatusCode);

          if (httpStatusCode < HttpURLConnection.HTTP_BAD_REQUEST) {
            connection.getInputStream().close();
          } else {
            LOG.debug("bad request");
            connection.getErrorStream().close();
          }
          LOG.debug("Response processed and closed");

          if (httpStatusCode >= HTTP_STATUS_CONTINUE) {
            String httpStatusString = String.valueOf(httpStatusCode);

            boolean shouldRollback = findOverrideValue(httpStatusString,
                rollbackOverrides, defaultRollback);

            if (shouldRollback) {
              txn.rollback();
            } else {
              txn.commit();
            }

            boolean shouldBackoff = findOverrideValue(httpStatusString,
                backoffOverrides, defaultBackoff);

            if (shouldBackoff) {
              status = Status.BACKOFF;
            } else {
              status = Status.READY;
            }

            boolean shouldIncrementMetrics = findOverrideValue(httpStatusString,
                incrementMetricsOverrides, defaultIncrementMetrics);

            if (shouldIncrementMetrics) {
              sinkCounter.incrementEventDrainSuccessCount();
            }

            if (shouldRollback) {
              if (shouldBackoff) {
                LOG.info(String.format("Got status code %d from HTTP server."
                    + " Rolled back event and backed off.", httpStatusCode));
              } else {
                LOG.info(String.format("Got status code %d from HTTP server."
                    + " Rolled back event for retry.", httpStatusCode));
              }
            }
          } else {
            txn.rollback();
            status = Status.BACKOFF;

            LOG.warn("Malformed response returned from server, retrying");
          }

        } catch (IOException e) {
          txn.rollback();
          status = Status.BACKOFF;

          LOG.error("Error opening connection, or request timed out", e);
        }

      } else {
        txn.commit();
        status = Status.BACKOFF;

        LOG.warn("Processed empty event");
      }

    } catch (Throwable t) {
      txn.rollback();
      status = Status.BACKOFF;

      LOG.error("Error sending HTTP request, retrying", t);

      // re-throw all Errors
      if (t instanceof Error) {
        throw (Error) t;
      }

    } finally {
      txn.close();

      if (outputStream != null) {
        try {
          outputStream.close();
        } catch (IOException e) {
          // ignore errors
        }
      }
    }

    return status;
  }

  /**
   * Reads a set of override values from the context configuration and stores
   * the results in the Map provided.
   *
   * @param propertyName  the prefix of the config property names
   * @param context       the context to use to read config properties
   * @param override      the override Map to store results in
   */
  private void parseConfigOverrides(final String propertyName,
                                    final Context context,
                                    final Map<String, Boolean> override) {

    ImmutableMap<String, String> config = context.getSubProperties(
        propertyName + ".");

    if (config != null) {
      for (Map.Entry<String, String> value : config.entrySet()) {
        LOG.info(String.format("Read %s value for status code %s as %s",
            propertyName, value.getKey(), value.getValue()));

        if (override.containsKey(value.getKey())) {
          LOG.warn(String.format("Ignoring duplicate config value for %s.%s",
              propertyName, value.getKey()));
        } else {
          override.put(value.getKey(), Boolean.valueOf(value.getValue()));
        }
      }
    }
  }

  /**
   * Queries the specified override map to find the most appropriate value. The
   * most specific match is found.
   *
   * @param statusCode    the String representation of the HTTP status code
   * @param overrides     the map of status code overrides
   * @param defaultValue  the default value to use if no override is configured
   *
   * @return the value of the most specific match to the given status code
   */
  private boolean findOverrideValue(final String statusCode,
                                    final HashMap<String, Boolean> overrides,
                                    final boolean defaultValue) {

    Boolean overrideValue = overrides.get(statusCode);
    if (overrideValue == null) {
      overrideValue = overrides.get(statusCode.substring(0, 1) + "XX");
      if (overrideValue == null) {
        overrideValue = defaultValue;
      }
    }
    return overrideValue;
  }

  /**
   * Update the connection builder.
   *
   * @param builder  the new value
   */
  final void setConnectionBuilder(final ConnectionBuilder builder) {
    this.connectionBuilder = builder;
  }

  /**
   * Update the sinkCounter.
   *
   * @param newSinkCounter  the new value
   */
  final void setSinkCounter(final SinkCounter newSinkCounter) {
    this.sinkCounter = newSinkCounter;
  }

  /**
   * Class used to allow extending the connection building functionality.
   */
  class ConnectionBuilder {

    /**
     * Creates an HTTP connection to the configured endpoint address. This
     * connection is setup for a POST request, and uses the content type and
     * accept header values in the configuration.
     *
     * @return the connection object
     * @throws IOException on any connection error
     */
    public HttpURLConnection getConnection() throws IOException {
      HttpURLConnection connection = (HttpURLConnection)
          endpointUrl.openConnection();

      connection.setRequestMethod("POST");
      connection.setRequestProperty("Content-Type", contentTypeHeader);
      connection.setRequestProperty("Accept", acceptHeader);
      connection.setConnectTimeout(connectTimeout);
      connection.setReadTimeout(requestTimeout);
      connection.setDoOutput(true);
      connection.setDoInput(true);
      connection.connect();
      return connection;
    }
  }
}
