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

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Sink.Status;
import org.apache.flume.Transaction;
import org.apache.flume.instrumentation.SinkCounter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestHttpSink {

  private static final Integer DEFAULT_REQUEST_TIMEOUT = 5000;
  private static final Integer DEFAULT_CONNECT_TIMEOUT = 5000;
  private static final String DEFAULT_ACCEPT_HEADER = "text/plain";
  private static final String DEFAULT_CONTENT_TYPE_HEADER = "text/plain";

  @Mock
  private SinkCounter sinkCounter;

  @Mock
  private Context configContext;

  @Mock
  private Channel channel;

  @Mock
  private Transaction transaction;

  @Mock
  private Event event;

  @Mock
  private HttpURLConnection httpURLConnection;

  @Mock
  private OutputStream outputStream;

  @Mock
  private InputStream inputStream;

  @Test
  public void ensureAllConfigurationOptionsRead() {
    whenDefaultStringConfig();
    whenDefaultBooleanConfig();
    when(configContext.getInteger(eq("connectTimeout"), Mockito.anyInt())).thenReturn(1000);
    when(configContext.getInteger(eq("requestTimeout"), Mockito.anyInt())).thenReturn(1000);

    new HttpSink().configure(configContext);

    verify(configContext).getString("endpoint", "");
    verify(configContext).getInteger(eq("connectTimeout"), Mockito.anyInt());
    verify(configContext).getInteger(eq("requestTimeout"), Mockito.anyInt());
    verify(configContext).getString(eq("acceptHeader"), Mockito.anyString());
    verify(configContext).getString(eq("contentTypeHeader"), Mockito.anyString());
    verify(configContext).getBoolean("defaultBackoff", true);
    verify(configContext).getBoolean("defaultRollback", true);
    verify(configContext).getBoolean("defaultIncrementMetrics", false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void ensureExceptionIfEndpointUrlEmpty() {
    when(configContext.getString("endpoint", "")).thenReturn("");
    new HttpSink().configure(configContext);
  }

  @Test(expected = IllegalArgumentException.class)
  public void ensureExceptionIfEndpointUrlInvalid() {
    when(configContext.getString("endpoint", "")).thenReturn("invalid url");
    new HttpSink().configure(configContext);
  }

  @Test(expected = IllegalArgumentException.class)
  public void ensureExceptionIfConnectTimeoutNegative() {
    whenDefaultStringConfig();
    when(configContext.getInteger("connectTimeout", 1000)).thenReturn(-1000);
    when(configContext.getInteger(eq("requestTimeout"), Mockito.anyInt())).thenReturn(1000);
    new HttpSink().configure(configContext);
  }

  @Test
  public void ensureDefaultConnectTimeoutCorrect() {
    whenDefaultStringConfig();
    when(configContext.getInteger("connectTimeout", DEFAULT_CONNECT_TIMEOUT)).thenReturn(1000);
    when(configContext.getInteger(eq("requestTimeout"), Mockito.anyInt())).thenReturn(1000);
    new HttpSink().configure(configContext);
    verify(configContext).getInteger("connectTimeout", DEFAULT_CONNECT_TIMEOUT);
  }

  @Test(expected = IllegalArgumentException.class)
  public void ensureExceptionIfRequestTimeoutNegative() {
    whenDefaultStringConfig();
    when(configContext.getInteger("requestTimeout", 1000)).thenReturn(-1000);
    when(configContext.getInteger(eq("connectTimeout"), Mockito.anyInt())).thenReturn(1000);
    new HttpSink().configure(configContext);
  }

  @Test
  public void ensureDefaultRequestTimeoutCorrect() {
    whenDefaultStringConfig();
    when(configContext.getInteger("requestTimeout", DEFAULT_REQUEST_TIMEOUT)).thenReturn(1000);
    when(configContext.getInteger(eq("connectTimeout"), Mockito.anyInt())).thenReturn(1000);
    new HttpSink().configure(configContext);
    verify(configContext).getInteger("requestTimeout", DEFAULT_REQUEST_TIMEOUT);
  }

  @Test
  public void ensureDefaultAcceptHeaderCorrect() {
    whenDefaultTimeouts();
    whenDefaultStringConfig();
    new HttpSink().configure(configContext);
    verify(configContext).getString("acceptHeader", DEFAULT_ACCEPT_HEADER);
  }

  @Test
  public void ensureDefaultContentTypeHeaderCorrect() {
    whenDefaultTimeouts();
    whenDefaultStringConfig();
    new HttpSink().configure(configContext);
    verify(configContext).getString("contentTypeHeader", DEFAULT_CONTENT_TYPE_HEADER);
  }

  @Test
  public void ensureBackoffOnNullEvent() throws Exception {
    when(channel.take()).thenReturn(null);
    executeWithMocks(true);
  }

  @Test
  public void ensureBackoffOnNullEventBody() throws Exception {
    when(channel.take()).thenReturn(event);
    when(event.getBody()).thenReturn(null);
    executeWithMocks(true);
  }

  @Test
  public void ensureBackoffOnEmptyEvent() throws Exception {
    when(channel.take()).thenReturn(event);
    when(event.getBody()).thenReturn(new byte[]{});
    executeWithMocks(true);
  }

  @Test
  public void ensureRollbackBackoffAndIncrementMetricsIfConfigured() throws Exception {
    when(channel.take()).thenReturn(event);
    when(event.getBody()).thenReturn("something".getBytes());

    Context context = new Context();
    context.put("defaultRollback", "true");
    context.put("defaultBackoff", "true");
    context.put("defaultIncrementMetrics", "true");

    executeWithMocks(false, Status.BACKOFF, true, true, context, HttpURLConnection.HTTP_OK);
  }

  @Test
  public void ensureCommitReadyAndNoIncrementMetricsIfConfigured() throws Exception {
    when(channel.take()).thenReturn(event);
    when(event.getBody()).thenReturn("something".getBytes());

    Context context = new Context();
    context.put("defaultRollback", "false");
    context.put("defaultBackoff", "false");
    context.put("defaultIncrementMetrics", "false");

    executeWithMocks(true, Status.READY, false, false, context, HttpURLConnection.HTTP_OK);
  }

  @Test
  public void ensureSingleStatusConfigurationCorrectlyUsed() throws Exception {
    when(channel.take()).thenReturn(event);
    when(event.getBody()).thenReturn("something".getBytes());

    Context context = new Context();
    context.put("defaultRollback", "true");
    context.put("defaultBackoff", "true");
    context.put("defaultIncrementMetrics", "false");
    context.put("rollback.200", "false");
    context.put("backoff.200", "false");
    context.put("incrementMetrics.200", "true");

    executeWithMocks(true, Status.READY, true, true, context, HttpURLConnection.HTTP_OK);
  }

  @Test
  public void ensureSingleErrorStatusConfigurationCorrectlyUsed() throws Exception {
    when(channel.take()).thenReturn(event);
    when(event.getBody()).thenReturn("something".getBytes());

    Context context = new Context();
    context.put("defaultRollback", "true");
    context.put("defaultBackoff", "true");
    context.put("defaultIncrementMetrics", "false");
    context.put("rollback.401", "false");
    context.put("backoff.401", "false");
    context.put("incrementMetrics.401", "false");

    executeWithMocks(true, Status.READY, false, true, context, HttpURLConnection.HTTP_UNAUTHORIZED);
  }

  @Test
  public void ensureGroupConfigurationCorrectlyUsed() throws Exception {
    when(channel.take()).thenReturn(event);
    when(event.getBody()).thenReturn("something".getBytes());

    Context context = new Context();
    context.put("defaultRollback", "true");
    context.put("defaultBackoff", "true");
    context.put("defaultIncrementMetrics", "false");
    context.put("rollback.2XX", "false");
    context.put("backoff.2XX", "false");
    context.put("incrementMetrics.2XX", "true");

    executeWithMocks(true, Status.READY, true, true, context, HttpURLConnection.HTTP_OK);
    executeWithMocks(true, Status.READY, true, true, context, HttpURLConnection.HTTP_NO_CONTENT);
  }

  @Test
  public void ensureSingleStatusConfigurationOverridesGroupConfigurationCorrectly()
      throws Exception {

    when(channel.take()).thenReturn(event);
    when(event.getBody()).thenReturn("something".getBytes());

    Context context = new Context();
    context.put("rollback.2XX", "false");
    context.put("backoff.2XX", "false");
    context.put("incrementMetrics.2XX", "true");
    context.put("rollback.200", "true");
    context.put("backoff.200", "true");
    context.put("incrementMetrics.200", "false");

    executeWithMocks(true, Status.READY, true, true, context, HttpURLConnection.HTTP_NO_CONTENT);
    executeWithMocks(false, Status.BACKOFF, false, true, context, HttpURLConnection.HTTP_OK);
  }

  private void executeWithMocks(boolean commit) throws Exception {
    Context context = new Context();
    executeWithMocks(commit, Status.BACKOFF, false, false, context, HttpURLConnection.HTTP_OK);
  }

  private void executeWithMocks(boolean expectedCommit, Status expectedStatus,
                                boolean expectedIncrementSuccessMetrics,
                                boolean expectedIncrementAttemptMetrics,
                                Context context, int httpStatus)
      throws Exception {

    context.put("endpoint", "http://localhost:8080/endpoint");

    HttpSink httpSink = new HttpSink();
    httpSink.configure(context);
    httpSink.setConnectionBuilder(httpSink.new ConnectionBuilder() {
      @Override
      public HttpURLConnection getConnection() throws IOException {
        return httpURLConnection;
      }
    });
    httpSink.setChannel(channel);
    httpSink.setSinkCounter(sinkCounter);

    when(channel.getTransaction()).thenReturn(transaction);
    when(httpURLConnection.getOutputStream()).thenReturn(outputStream);
    when(httpURLConnection.getInputStream()).thenReturn(inputStream);
    when(httpURLConnection.getErrorStream()).thenReturn(inputStream);
    when(httpURLConnection.getResponseCode()).thenReturn(httpStatus);

    Status actualStatus = httpSink.process();

    assert (actualStatus == expectedStatus);

    inOrder(transaction).verify(transaction).begin();

    if (expectedIncrementAttemptMetrics) {
      inOrder(sinkCounter).verify(sinkCounter).incrementEventDrainAttemptCount();
    }

    if (expectedCommit) {
      inOrder(transaction).verify(transaction).commit();
    } else {
      inOrder(transaction).verify(transaction).rollback();
    }

    if (expectedIncrementSuccessMetrics) {
      inOrder(sinkCounter).verify(sinkCounter).incrementEventDrainSuccessCount();
    }

    inOrder(transaction).verify(transaction).close();
  }

  private void whenDefaultStringConfig() {
    when(configContext.getString("endpoint", "")).thenReturn("http://test.abc/");
    when(configContext.getString("acceptHeader", "")).thenReturn("test/accept");
    when(configContext.getString("contentTypeHeader", "")).thenReturn("test/content");
  }

  private void whenDefaultBooleanConfig() {
    when(configContext.getBoolean("defaultBackoff", true)).thenReturn(true);
    when(configContext.getBoolean("defaultRollback", true)).thenReturn(true);
    when(configContext.getBoolean("defaultIncrementMetrics", false)).thenReturn(true);
  }

  private void whenDefaultTimeouts() {
    when(configContext.getInteger(eq("requestTimeout"), Mockito.anyInt())).thenReturn(1000);
    when(configContext.getInteger(eq("connectTimeout"), Mockito.anyInt())).thenReturn(1000);
  }
}
