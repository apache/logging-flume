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

import com.github.tomakehurst.wiremock.global.RequestDelaySpec;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.RequestListener;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.event.SimpleEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.apache.flume.Sink.Status;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Runs a set of tests against a mocked HTTP endpoint.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestHttpSinkIT {

  private static final int RESPONSE_TIMEOUT = 4000;
  private static final int CONNECT_TIMEOUT = 2500;

  private MemoryChannel channel;

  private HttpSink httpSink;

  @Before
  public void setupSink() {
    if (httpSink == null) {
      Context httpSinkContext = new Context();
      httpSinkContext.put("endpoint", "http://localhost:8080/endpoint");
      httpSinkContext.put("requestTimeout", "2000");
      httpSinkContext.put("connectTimeout", "1500");
      httpSinkContext.put("acceptHeader", "application/json");
      httpSinkContext.put("contentTypeHeader", "application/json");
      httpSinkContext.put("backoff.200", "false");
      httpSinkContext.put("rollback.200", "false");
      httpSinkContext.put("backoff.401", "false");
      httpSinkContext.put("rollback.401", "false");
      httpSinkContext.put("incrementMetrics.200", "true");

      Context memoryChannelContext = new Context();

      channel = new MemoryChannel();
      channel.configure(memoryChannelContext);
      channel.start();

      httpSink = new HttpSink();
      httpSink.configure(httpSinkContext);
      httpSink.setChannel(channel);
      httpSink.start();
    }
  }

  @After
  public void waitForShutdown() throws InterruptedException {
    httpSink.stop();
    new CountDownLatch(1).await(500, TimeUnit.MILLISECONDS);
  }

  @Rule
  public WireMockRule service = new WireMockRule(wireMockConfig().port(8080));

  @Test
  public void ensureSuccessfulMessageDelivery() throws Exception {
    service.stubFor(post(urlEqualTo("/endpoint"))
        .withRequestBody(equalToJson(event("SUCCESS")))
        .willReturn(aResponse().withStatus(200)));

    addEventToChannel(event("SUCCESS"));

    service.verify(1, postRequestedFor(urlEqualTo("/endpoint"))
        .withRequestBody(equalToJson(event("SUCCESS"))));
  }

  @Test
  public void ensureEventsResentOn503Failure() throws Exception {
    String errorScenario = "Error Scenario";

    service.stubFor(post(urlEqualTo("/endpoint"))
        .inScenario(errorScenario)
        .whenScenarioStateIs(STARTED)
        .withRequestBody(equalToJson(event("TRANSIENT_ERROR")))
        .willReturn(aResponse().withStatus(503))
        .willSetStateTo("Error Sent"));

    service.stubFor(post(urlEqualTo("/endpoint"))
        .inScenario(errorScenario)
        .whenScenarioStateIs("Error Sent")
        .withRequestBody(equalToJson(event("TRANSIENT_ERROR")))
        .willReturn(aResponse().withStatus(200)));

    addEventToChannel(event("TRANSIENT_ERROR"), Status.BACKOFF);
    addEventToChannel(event("TRANSIENT_ERROR"), Status.READY);

    service.verify(2, postRequestedFor(urlEqualTo("/endpoint"))
        .withRequestBody(equalToJson(event("TRANSIENT_ERROR"))));
  }

  @Test
  public void ensureEventsNotResentOn401Failure() throws Exception {
    String errorScenario = "Error skip scenario";

    service.stubFor(post(urlEqualTo("/endpoint"))
            .inScenario(errorScenario)
            .whenScenarioStateIs(STARTED)
            .withRequestBody(equalToJson(event("UNAUTHORIZED REQUEST")))
            .willReturn(aResponse().withStatus(401)
            .withHeader("Content-Type", "text/plain")
            .withBody("Not allowed!"))
            .willSetStateTo("Error Sent"));

    service.stubFor(post(urlEqualTo("/endpoint"))
            .inScenario(errorScenario)
            .whenScenarioStateIs("Error Sent")
            .withRequestBody(equalToJson(event("NEXT EVENT")))
            .willReturn(aResponse().withStatus(200)));

    addEventToChannel(event("UNAUTHORIZED REQUEST"), Status.READY);
    addEventToChannel(event("NEXT EVENT"), Status.READY);

    service.verify(1, postRequestedFor(urlEqualTo("/endpoint"))
            .withRequestBody(equalToJson(event("UNAUTHORIZED REQUEST"))));

    service.verify(1, postRequestedFor(urlEqualTo("/endpoint"))
            .withRequestBody(equalToJson(event("NEXT EVENT"))));

  }

  @Test
  public void ensureEventsResentOnNetworkFailure() throws Exception {
    String errorScenario = "Error Scenario";

    service.stubFor(post(urlEqualTo("/endpoint"))
        .inScenario(errorScenario)
        .whenScenarioStateIs(STARTED)
        .withRequestBody(equalToJson(event("NETWORK_ERROR")))
        .willReturn(aResponse().withFault(Fault.RANDOM_DATA_THEN_CLOSE))
        .willSetStateTo("Error Sent"));

    service.stubFor(post(urlEqualTo("/endpoint"))
        .inScenario(errorScenario)
        .whenScenarioStateIs("Error Sent")
        .withRequestBody(equalToJson(event("NETWORK_ERROR")))
        .willReturn(aResponse().withStatus(200)));

    addEventToChannel(event("NETWORK_ERROR"), Status.BACKOFF);
    addEventToChannel(event("NETWORK_ERROR"), Status.READY);

    service.verify(2, postRequestedFor(urlEqualTo("/endpoint"))
        .withRequestBody(equalToJson(event("NETWORK_ERROR"))));
  }

  @Test
  public void ensureEventsResentOnConnectionTimeout() throws Exception {
    final CountDownLatch firstRequestReceived = new CountDownLatch(1);

    service.addSocketAcceptDelay(new RequestDelaySpec(CONNECT_TIMEOUT));
    service.addMockServiceRequestListener(new RequestListener() {
      @Override
      public void requestReceived(Request request, Response response) {
        service.addSocketAcceptDelay(new RequestDelaySpec(0));
        firstRequestReceived.countDown();
      }
    });

    service.stubFor(post(urlEqualTo("/endpoint"))
        .withRequestBody(equalToJson(event("SLOW_SOCKET")))
        .willReturn(aResponse().withStatus(200)));

    addEventToChannel(event("SLOW_SOCKET"), Status.BACKOFF);

    // wait until the socket is connected
    firstRequestReceived.await(2000, TimeUnit.MILLISECONDS);

    addEventToChannel(event("SLOW_SOCKET"), Status.READY);

    service.verify(2, postRequestedFor(urlEqualTo("/endpoint"))
        .withRequestBody(equalToJson(event("SLOW_SOCKET"))));
  }

  @Test
  public void ensureEventsResentOnRequestTimeout() throws Exception {
    String errorScenario = "Error Scenario";

    service.stubFor(post(urlEqualTo("/endpoint"))
        .inScenario(errorScenario)
        .whenScenarioStateIs(STARTED)
        .withRequestBody(equalToJson(event("SLOW_RESPONSE")))
        .willReturn(aResponse().withFixedDelay(RESPONSE_TIMEOUT).withStatus(200))
        .willSetStateTo("Slow Response Sent"));

    service.stubFor(post(urlEqualTo("/endpoint"))
        .inScenario(errorScenario)
        .whenScenarioStateIs("Slow Response Sent")
        .withRequestBody(equalToJson(event("SLOW_RESPONSE")))
        .willReturn(aResponse().withStatus(200)));

    addEventToChannel(event("SLOW_RESPONSE"), Status.BACKOFF);
    addEventToChannel(event("SLOW_RESPONSE"), Status.READY);

    service.verify(2, postRequestedFor(urlEqualTo("/endpoint"))
        .withRequestBody(equalToJson(event("SLOW_RESPONSE"))));
  }

  @Test
  public void ensureHttpConnectionReusedForSuccessfulRequests() throws Exception {
    // we should only get one delay when establishing a connection
    service.addSocketAcceptDelay(new RequestDelaySpec(1000));

    service.stubFor(post(urlEqualTo("/endpoint"))
        .withRequestBody(equalToJson(event("SUCCESS")))
        .willReturn(aResponse().withStatus(200)));

    long startTime = System.currentTimeMillis();

    addEventToChannel(event("SUCCESS"), Status.READY);
    addEventToChannel(event("SUCCESS"), Status.READY);
    addEventToChannel(event("SUCCESS"), Status.READY);

    long endTime = System.currentTimeMillis();
    assertTrue("Test should have completed faster", endTime - startTime < 2500);

    service.verify(3, postRequestedFor(urlEqualTo("/endpoint"))
        .withRequestBody(equalToJson(event("SUCCESS"))));
  }

  private void addEventToChannel(String line) throws EventDeliveryException {
    addEventToChannel(line, Status.READY);
  }

  private void addEventToChannel(String line, Status expectedStatus)
      throws EventDeliveryException {

    SimpleEvent event = new SimpleEvent();
    event.setBody(line.getBytes());

    Transaction channelTransaction = channel.getTransaction();
    channelTransaction.begin();
    channel.put(event);
    channelTransaction.commit();
    channelTransaction.close();

    Sink.Status status = httpSink.process();

    assertEquals(expectedStatus, status);
  }

  private String event(String id) {
    return "{'id':'" + id + "'}";
  }
}
