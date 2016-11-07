package uk.gov.hmrc.flume.sink;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig;
import static com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED;
import static org.mockito.Mockito.*;
import static org.apache.flume.Sink.Status;

import com.github.tomakehurst.wiremock.global.RequestDelaySpec;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.RequestListener;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.flume.*;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.instrumentation.SinkCounter;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Runs a set of tests against a correctly configured external running Flume
 * instance.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestHttpSinkIT {

    private static final int RESPONSE_TIMEOUT = 4000;
    private static final int CONNECT_TIMEOUT = 2000;

    private HttpSink httpSink;

    @Mock
    private SinkCounter sinkCounter;

    @Mock
    private Channel channel;

    @Mock
    private Transaction transaction;

    @Before
    public void setupSink() {
        if (httpSink == null) {
            Context context = new Context();
            context.put("endpoint", "http://localhost:8080/endpoint");
            context.put("requestTimeout", "2000");
            context.put("connectTimeout", "1500");
            context.put("acceptHeader", "application/json");
            context.put("contentTypeHeader", "application/json");
            context.put("backoff.200", "false");
            context.put("rollback.200", "false");
            context.put("incrementMetrics.200", "true");

            httpSink = new HttpSink();
            httpSink.configure(context);
            httpSink.setChannel(channel);
            httpSink.setSinkCounter(sinkCounter);
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

        addEventToChannel(event("TRANSIENT_ERROR"), false, Status.BACKOFF);
        addEventToChannel(event("TRANSIENT_ERROR"), true, Status.READY);

        service.verify(2, postRequestedFor(urlEqualTo("/endpoint"))
                .withRequestBody(equalToJson(event("TRANSIENT_ERROR"))));
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

        addEventToChannel(event("NETWORK_ERROR"), false, Status.BACKOFF);
        addEventToChannel(event("NETWORK_ERROR"), true, Status.READY);

        service.verify(2, postRequestedFor(urlEqualTo("/endpoint"))
                .withRequestBody(equalToJson(event("NETWORK_ERROR"))));
    }

    @Test
    public void ensureEventsResentOnConnectionTimeout() throws Exception {
        service.addSocketAcceptDelay(new RequestDelaySpec(CONNECT_TIMEOUT));
        service.addMockServiceRequestListener(new RequestListener() {
            @Override
            public void requestReceived(Request request, Response response) {
                service.addSocketAcceptDelay(new RequestDelaySpec(0));
            }
        });

        service.stubFor(post(urlEqualTo("/endpoint"))
                .withRequestBody(equalToJson(event("SLOW_SOCKET")))
                .willReturn(aResponse().withStatus(200)));

        addEventToChannel(event("SLOW_SOCKET"), false, Status.BACKOFF);
        addEventToChannel(event("SLOW_SOCKET"), true, Status.READY);

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

        addEventToChannel(event("SLOW_RESPONSE"), false, Status.BACKOFF);
        addEventToChannel(event("SLOW_RESPONSE"), true, Status.READY);

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

        addEventToChannel(event("SUCCESS"), true, Status.READY);
        addEventToChannel(event("SUCCESS"), true, Status.READY);
        addEventToChannel(event("SUCCESS"), true, Status.READY);

        long endTime = System.currentTimeMillis();
        assert(endTime - startTime < 2500);

        service.verify(3, postRequestedFor(urlEqualTo("/endpoint"))
                .withRequestBody(equalToJson(event("SUCCESS"))));
    }

    private void addEventToChannel(String line) throws EventDeliveryException {
        addEventToChannel(line, true, Status.READY);
    }

    private void addEventToChannel(String line, boolean commitTx, Status expectedStatus)
            throws EventDeliveryException {

        SimpleEvent event = new SimpleEvent();
        event.setBody(line.getBytes());

        when(channel.getTransaction()).thenReturn(transaction);
        when(channel.take()).thenReturn(event);

        Sink.Status status = httpSink.process();

        assert(status == expectedStatus);

        if (commitTx) {
            inOrder(transaction).verify(transaction).commit();
        } else {
            inOrder(transaction).verify(transaction).rollback();
        }
    }

    private String event(String id) {
        return "{'id':'" + id + "'}";
    }
}
