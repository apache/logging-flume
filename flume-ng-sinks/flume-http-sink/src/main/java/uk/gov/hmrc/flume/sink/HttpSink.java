package uk.gov.hmrc.flume.sink;

import com.google.common.collect.ImmutableMap;
import org.apache.flume.*;
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
 * HTTP Sink Implementation for Apache Flume.
 *
 * Behaviour of this sink is that it will take events from the channel, and
 * send those events to a remote service using an HTTP POST request. The event
 * content is sent as the POST body.
 *
 * Configurable options are :
 * endpoint - the fully qualified URL endpoint to POST to (required, no default)
 * connectTimeout - the socket connection timeout (default 5000ms)
 * requestTimeout - the maximum request processing time (default 5000ms)
 * contentTypeHeader - the HTTP Content-Type header (default text/plain)
 * acceptHeader - the HTTP Accept header value (default text/plain)
 * defaultBackoff - whether the default result for event processing is to BACKOFF (default false)
 * defaultRollback - whether by default the event processing transaction is rolled back (default false)
 * defaultIncrementMetrics - whether by default the event contributes to the sink metrics (default true)
 * backoff.[code] - whether to return a BACKOFF (if true) or READY (if false) result when the given HTTP response code
 *                  is returned from the remote host
 * rollback.[code] - whether the transaction is rolled back (if true) or committed (if false) when the given HTTP
 *                   response code is returned from the remote host
 * incrementMetrics.[code] - whether to increment the sink metrics when the given HTTP response code is returned from
 *                           the remote host
 *
 * Note that the values for [code] above can be either a single HTTP status code, i.e. 503 or 200, or a group of one
 * hundred status codes, i.e. 2XX or 5XX. If a single code and a group are specified, then the most specific result is
 * used. So for example both 503 and 5XX may be defined, and the 5XX case will apply for all codes in that range except
 * 503.
 *
 * Any malformed HTTP response returned by the server where the status code is
 * not readable will result in a backoff signal and the event is not consumed
 * from the channel.
 *
 * Any empty or null events are consumed without any request being made to the HTTP endpoint.
 */
public class HttpSink extends AbstractSink implements Configurable {

    private static final Logger LOG = Logger.getLogger(HttpSink.class);

    private static final int DEFAULT_CONNECT_TIMEOUT = 5000;
    private static final int DEFAULT_REQUEST_TIMEOUT = 5000;
    private static final String DEFAULT_CONTENT_TYPE = "text/plain";
    private static final String DEFAULT_ACCEPT_HEADER = "text/plain";

    private URL endpointUrl;
    private HttpURLConnection httpClient;
    private SinkCounter sinkCounter;

    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private int requestTimeout = DEFAULT_REQUEST_TIMEOUT;
    private String contentTypeHeader = DEFAULT_CONTENT_TYPE;
    private String acceptHeader = DEFAULT_ACCEPT_HEADER;
    private boolean defaultBackoff;
    private boolean defaultRollback;
    private boolean defaultIncrementMetrics;

    private HashMap<String, Boolean> backoffOverrides = new HashMap<>();
    private HashMap<String, Boolean> rollbackOverrides = new HashMap<>();
    private HashMap<String, Boolean> incrementMetricsOverrides = new HashMap<>();

    public void configure(Context context) {
        String configuredEndpoint = context.getString("endpoint", "");
        LOG.info("Read endpoint URL from configuration : " + configuredEndpoint);

        try {
            endpointUrl = new URL(configuredEndpoint);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Endpoint URL invalid", e);
        }

        connectTimeout = context.getInteger("connectTimeout", DEFAULT_CONNECT_TIMEOUT);
        if (connectTimeout <= 0) {
            throw new IllegalArgumentException("Connect timeout must be a non-zero and positive");
        }
        LOG.info("Using connect timeout : " + connectTimeout);

        requestTimeout = context.getInteger("requestTimeout", DEFAULT_REQUEST_TIMEOUT);
        if (requestTimeout <= 0) {
            throw new IllegalArgumentException("Request timeout must be a non-zero and positive");
        }
        LOG.info("Using request timeout : " + requestTimeout);

        acceptHeader = context.getString("acceptHeader", DEFAULT_ACCEPT_HEADER);
        LOG.info("Using Accept header value : " + acceptHeader);

        contentTypeHeader = context.getString("contentTypeHeader", DEFAULT_CONTENT_TYPE);
        LOG.info("Using Content-Type header value : " + contentTypeHeader);

        defaultBackoff = context.getBoolean("defaultBackoff", true);
        LOG.info("Channel backoff by default is " + Boolean.toString(defaultBackoff));

        defaultRollback = context.getBoolean("defaultRollback", true);
        LOG.info("Transaction rollback by default is " + Boolean.toString(defaultRollback));

        defaultIncrementMetrics = context.getBoolean("defaultIncrementMetrics", false);
        LOG.info("Incrementing metrics by default is " + Boolean.toString(defaultIncrementMetrics));

        parseConfigOverrides("backoff", context, backoffOverrides);
        parseConfigOverrides("rollback", context, rollbackOverrides);
        parseConfigOverrides("incrementMetrics", context, incrementMetricsOverrides);

        if(this.sinkCounter == null) {
            this.sinkCounter = new SinkCounter(this.getName());
        }
    }

    @Override
    public void start() {
        LOG.info("Starting HttpSink");
        sinkCounter.start();
    }

    @Override
    public void stop() {
        LOG.info("Stopping HttpSink");
        sinkCounter.stop();
    }

    public Status process() throws EventDeliveryException {
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
                    httpClient = getConnection();

                    outputStream = httpClient.getOutputStream();
                    outputStream.write(eventBody);
                    outputStream.flush();
                    outputStream.close();

                    int httpStatusCode = httpClient.getResponseCode();
                    LOG.debug("Got status code : " + httpStatusCode);

                    httpClient.getInputStream().close();
                    LOG.debug("Response processed and closed");

                    if (httpStatusCode >= 100) {
                        String httpStatusString = String.valueOf(httpStatusCode);

                        boolean shouldRollback = findOverrideValue(httpStatusString, rollbackOverrides, defaultRollback);
                        if (shouldRollback) {
                            txn.rollback();
                        } else {
                            txn.commit();
                        }

                        boolean shouldBackoff = findOverrideValue(httpStatusString, backoffOverrides, defaultBackoff);
                        if (shouldBackoff) {
                            status = Status.BACKOFF;
                        } else {
                            status = Status.READY;
                        }

                        boolean shouldIncrementMetrics = findOverrideValue(httpStatusString, incrementMetricsOverrides, defaultIncrementMetrics);
                        if (shouldIncrementMetrics) {
                            sinkCounter.incrementEventDrainSuccessCount();
                        }

                        if (shouldRollback) {
                            if (shouldBackoff) {
                                LOG.info(String.format("Got status code %d from HTTP server. Rolled back event and backed off.", httpStatusCode));
                            } else {
                                LOG.info(String.format("Got status code %d from HTTP server. Rolled back event for retry.", httpStatusCode));
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
                throw (Error)t;
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

    private void parseConfigOverrides(String propertyName, Context context, Map<String, Boolean> override) {
        ImmutableMap<String, String> config = context.getSubProperties(propertyName + ".");

        if (config != null) {
            for (Map.Entry<String, String> value : config.entrySet()) {
                LOG.info(String.format("Read %s value for status code %s as %s", propertyName, value.getKey(), value.getValue()));

                if (override.containsKey(value.getKey())) {
                    LOG.warn(String.format("Ignoring duplicate config value for %s.%s", propertyName, value.getKey()));
                } else {
                    override.put(value.getKey(), Boolean.valueOf(value.getValue()));
                }
            }
        }
    }

    private boolean findOverrideValue(String statusCode, HashMap<String, Boolean> overrides, boolean defaultValue) {
        Boolean overrideValue = overrides.get(statusCode);
        if (overrideValue == null) {
            overrideValue = overrides.get(statusCode.substring(0, 1) + "XX");
            if (overrideValue == null) {
                overrideValue = defaultValue;
            }
        }
        return overrideValue;
    }

    HttpURLConnection getConnection() throws IOException {
        HttpURLConnection connection = (HttpURLConnection) endpointUrl.openConnection();
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

    void setSinkCounter(SinkCounter sinkCounter) {
        this.sinkCounter = sinkCounter;
    }
}
