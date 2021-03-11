/*
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
package org.apache.flume.source.http;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.HttpResponse;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.fest.reflect.core.Reflection.field;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doThrow;

/**
 *
 */
public class TestHTTPSource {

  private static HTTPSource httpSource;
  private static HTTPSource httpsSource;
  private static HTTPSource httpsGlobalKeystoreSource;

  private static Channel httpChannel;
  private static Channel httpsChannel;
  private static Channel httpsGlobalKeystoreChannel;
  private static int httpPort;
  private static int httpsPort;
  private static int httpsGlobalKeystorePort;
  private HttpClient httpClient;
  private HttpPost postRequest;

  private static int findFreePort() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  private static Context getDefaultNonSecureContext(int port) throws IOException {
    Context ctx = new Context();
    ctx.put(HTTPSourceConfigurationConstants.CONFIG_BIND, "0.0.0.0");
    ctx.put(HTTPSourceConfigurationConstants.CONFIG_PORT, String.valueOf(port));
    ctx.put("QueuedThreadPool.MaxThreads", "100");
    return ctx;
  }

  private static Context getDefaultSecureContext(int port) throws IOException {
    Context sslContext = new Context();
    sslContext.put(HTTPSourceConfigurationConstants.CONFIG_PORT, String.valueOf(port));
    sslContext.put(HTTPSourceConfigurationConstants.SSL_ENABLED, "true");
    sslContext.put(HTTPSourceConfigurationConstants.SSL_KEYSTORE_PASSWORD, "password");
    sslContext.put(HTTPSourceConfigurationConstants.SSL_KEYSTORE,
                   "src/test/resources/jettykeystore");
    return sslContext;
  }

  private static Context getDefaultSecureContextGlobalKeystore(int port) throws IOException {
    System.setProperty("javax.net.ssl.keyStore", "src/test/resources/jettykeystore");
    System.setProperty("javax.net.ssl.keyStorePassword", "password");

    Context sslContext = new Context();
    sslContext.put(HTTPSourceConfigurationConstants.CONFIG_PORT, String.valueOf(port));
    sslContext.put(HTTPSourceConfigurationConstants.SSL_ENABLED, "true");
    return sslContext;
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    httpSource = new HTTPSource();
    httpChannel = new MemoryChannel();
    httpPort = findFreePort();
    configureSourceAndChannel(httpSource, httpChannel, getDefaultNonSecureContext(httpPort));
    httpChannel.start();
    httpSource.start();

    httpsSource = new HTTPSource();
    httpsChannel = new MemoryChannel();
    httpsPort = findFreePort();
    configureSourceAndChannel(httpsSource, httpsChannel, getDefaultSecureContext(httpsPort));
    httpsChannel.start();
    httpsSource.start();

    httpsGlobalKeystoreSource = new HTTPSource();
    httpsGlobalKeystoreChannel = new MemoryChannel();
    httpsGlobalKeystorePort = findFreePort();
    configureSourceAndChannel(httpsGlobalKeystoreSource, httpsGlobalKeystoreChannel,
        getDefaultSecureContextGlobalKeystore(httpsGlobalKeystorePort));
    httpsGlobalKeystoreChannel.start();
    httpsGlobalKeystoreSource.start();

    System.clearProperty("javax.net.ssl.keyStore");
    System.clearProperty("javax.net.ssl.keyStorePassword");
  }

  private static void configureSourceAndChannel(
      HTTPSource source, Channel channel, Context context
  ) {
    Context channelContext = new Context();
    channelContext.put("capacity", "100");
    Configurables.configure(channel, channelContext);
    Configurables.configure(source, context);

    ChannelSelector rcs1 = new ReplicatingChannelSelector();
    rcs1.setChannels(Collections.singletonList(channel));

    source.setChannelProcessor(new ChannelProcessor(rcs1));
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    httpSource.stop();
    httpChannel.stop();
    httpsSource.stop();
    httpsChannel.stop();
    httpsGlobalKeystoreSource.stop();
    httpsGlobalKeystoreChannel.stop();
  }

  @Before
  public void setUp() {
    HttpClientBuilder builder = HttpClientBuilder.create();
    httpClient = builder.build();
    postRequest = new HttpPost("http://0.0.0.0:" + httpPort);
    SourceCounter sc = (SourceCounter) Whitebox.getInternalState(httpSource, "sourceCounter");
    sc.start();
  }

  @After
  public void tearDown() {
    SourceCounter sc = (SourceCounter) Whitebox.getInternalState(httpSource, "sourceCounter");
    sc.stop();
  }

  @Test
  public void testSimple() throws IOException, InterruptedException {

    StringEntity input = new StringEntity("[{\"headers\":{\"a\": \"b\"},\"body\": \"random_body\"},"
            + "{\"headers\":{\"e\": \"f\"},\"body\": \"random_body2\"}]");
    //if we do not set the content type to JSON, the client will use
    //ISO-8859-1 as the charset. JSON standard does not support this.
    input.setContentType("application/json");
    postRequest.setEntity(input);

    HttpResponse response = httpClient.execute(postRequest);

    Assert.assertEquals(HttpServletResponse.SC_OK,
            response.getStatusLine().getStatusCode());
    Transaction tx = httpChannel.getTransaction();
    tx.begin();
    Event e = httpChannel.take();
    Assert.assertNotNull(e);
    Assert.assertEquals("b", e.getHeaders().get("a"));
    Assert.assertEquals("random_body", new String(e.getBody(), "UTF-8"));

    e = httpChannel.take();
    Assert.assertNotNull(e);
    Assert.assertEquals("f", e.getHeaders().get("e"));
    Assert.assertEquals("random_body2", new String(e.getBody(), "UTF-8"));
    tx.commit();
    tx.close();
  }

  @Test
  public void testTrace() throws Exception {
    doTestForbidden(new HttpTrace("http://0.0.0.0:" + httpPort));
  }

  @Test
  public void testOptions() throws Exception {
    doTestForbidden(new HttpOptions("http://0.0.0.0:" + httpPort));
  }

  private void doTestForbidden(HttpRequestBase request) throws Exception {
    HttpResponse response = httpClient.execute(request);
    Assert.assertEquals(HttpServletResponse.SC_FORBIDDEN,
                        response.getStatusLine().getStatusCode());
  }

  @Test
  public void testSimpleUTF16() throws IOException, InterruptedException {

    StringEntity input = new StringEntity("[{\"headers\":{\"a\": \"b\"},\"body\": \"random_body\"},"
            + "{\"headers\":{\"e\": \"f\"},\"body\": \"random_body2\"}]", "UTF-16");
    input.setContentType("application/json; charset=utf-16");
    postRequest.setEntity(input);

    HttpResponse response = httpClient.execute(postRequest);

    Assert.assertEquals(HttpServletResponse.SC_OK,
            response.getStatusLine().getStatusCode());
    Transaction tx = httpChannel.getTransaction();
    tx.begin();
    Event e = httpChannel.take();
    Assert.assertNotNull(e);
    Assert.assertEquals("b", e.getHeaders().get("a"));
    Assert.assertEquals("random_body", new String(e.getBody(), "UTF-16"));

    e = httpChannel.take();
    Assert.assertNotNull(e);
    Assert.assertEquals("f", e.getHeaders().get("e"));
    Assert.assertEquals("random_body2", new String(e.getBody(), "UTF-16"));
    tx.commit();
    tx.close();
  }

  @Test
  public void testInvalid() throws Exception {
    StringEntity input = new StringEntity("[{\"a\": \"b\",[\"d\":\"e\"],\"body\": \"random_body\"},"
            + "{\"e\": \"f\",\"body\": \"random_body2\"}]");
    input.setContentType("application/json");
    postRequest.setEntity(input);
    HttpResponse response = httpClient.execute(postRequest);

    Assert.assertEquals(HttpServletResponse.SC_BAD_REQUEST,
            response.getStatusLine().getStatusCode());
    SourceCounter sc = (SourceCounter) Whitebox.getInternalState(httpSource, "sourceCounter");
    Assert.assertEquals(1, sc.getEventReadFail());

  }

  @Test
  public void testBigBatchDeserializationUTF8() throws Exception {
    testBatchWithVariousEncoding("UTF-8");
  }

  @Test
  public void testBigBatchDeserializationUTF16() throws Exception {
    testBatchWithVariousEncoding("UTF-16");
  }

  @Test
  public void testBigBatchDeserializationUTF32() throws Exception {
    testBatchWithVariousEncoding("UTF-32");
  }

  @Test
  public void testCounterGenericFail() throws Exception {
    ChannelProcessor cp = Mockito.mock(ChannelProcessor.class);
    doThrow(new RuntimeException("dummy")).when(cp).processEventBatch(anyListOf(Event.class));
    ChannelProcessor oldCp = httpSource.getChannelProcessor();
    httpSource.setChannelProcessor(cp);
    testBatchWithVariousEncoding("UTF-8");
    SourceCounter sc = (SourceCounter) Whitebox.getInternalState(httpSource, "sourceCounter");
    Assert.assertEquals(1, sc.getGenericProcessingFail());
    httpSource.setChannelProcessor(oldCp);
  }

  @Test
  public void testSingleEvent() throws Exception {
    StringEntity input = new StringEntity("[{\"headers\" : {\"a\": \"b\"},\"body\":"
            + " \"random_body\"}]");
    input.setContentType("application/json");
    postRequest.setEntity(input);

    httpClient.execute(postRequest);
    Transaction tx = httpChannel.getTransaction();
    tx.begin();
    Event e = httpChannel.take();
    Assert.assertNotNull(e);
    Assert.assertEquals("b", e.getHeaders().get("a"));
    Assert.assertEquals("random_body", new String(e.getBody(),"UTF-8"));
    tx.commit();
    tx.close();
  }

  /**
   * First test that the unconfigured behaviour is as-expected, then add configurations
   * to a new channel and observe the difference.
   * For some of the properties, the most convenient way to test is using the MBean interface
   * We test all of HttpConfiguration, ServerConnector, QueuedThreadPool and SslContextFactory
   * sub-configurations (but not all properties)
   */
  @Test
  public void testConfigurables() throws Exception {
    StringEntity input = new StringEntity("[{\"headers\" : {\"a\": \"b\"},\"body\":"
            + " \"random_body\"}]");
    input.setContentType("application/json");
    postRequest.setEntity(input);

    HttpResponse resp = httpClient.execute(postRequest);

    // Testing default behaviour (to not provided X-Powered-By, but to provide Server headers)
    Assert.assertTrue(resp.getHeaders("X-Powered-By").length == 0);
    Assert.assertTrue(resp.getHeaders("Server").length == 1);

    Transaction tx = httpChannel.getTransaction();
    tx.begin();
    Event e = httpChannel.take();
    Assert.assertNotNull(e);
    tx.commit();
    tx.close();
    Assert.assertTrue(findMBeans("org.eclipse.jetty.util.thread:type=queuedthreadpool,*",
        "maxThreads", 123).size() == 0);
    Assert.assertTrue(findMBeans("org.eclipse.jetty.server:type=serverconnector,*",
        "acceptQueueSize", 22).size() == 0);

    int newPort = findFreePort();
    Context configuredSourceContext = getDefaultNonSecureContext(newPort);
    configuredSourceContext.put("HttpConfiguration.sendServerVersion", "false");
    configuredSourceContext.put("HttpConfiguration.sendXPoweredBy", "true");
    configuredSourceContext.put("ServerConnector.acceptQueueSize", "22");
    configuredSourceContext.put("QueuedThreadPool.maxThreads", "123");

    HTTPSource newSource = new HTTPSource();
    Channel newChannel = new MemoryChannel();
    configureSourceAndChannel(newSource, newChannel, configuredSourceContext);
    newChannel.start();
    newSource.start();

    HttpPost newPostRequest = new HttpPost("http://0.0.0.0:" + newPort);

    resp = httpClient.execute(newPostRequest);
    Assert.assertTrue(resp.getHeaders("X-Powered-By").length > 0);
    Assert.assertTrue(resp.getHeaders("Server").length == 0);
    Assert.assertTrue(findMBeans("org.eclipse.jetty.util.thread:type=queuedthreadpool,*",
        "maxThreads", 123).size() == 1);
    Assert.assertTrue(findMBeans("org.eclipse.jetty.server:type=serverconnector,*",
        "acceptQueueSize", 22).size() == 1);

    newSource.stop();
    newChannel.stop();

    //Configure SslContextFactory with junk protocols (expect failure)
    newPort = findFreePort();
    configuredSourceContext = getDefaultSecureContext(newPort);
    configuredSourceContext.put("SslContextFactory.IncludeProtocols", "abc def");

    newSource = new HTTPSource();
    newChannel = new MemoryChannel();

    configureSourceAndChannel(newSource, newChannel, configuredSourceContext);

    newChannel.start();
    newSource.start();

    newPostRequest = new HttpPost("http://0.0.0.0:" + newPort);
    try {
      doTestHttps(null, newPort, httpsChannel);
      //We are testing that this fails because we've deliberately configured the wrong protocols
      Assert.assertTrue(false);
    } catch (AssertionError ex) {
      //no-op
    }
    newSource.stop();
    newChannel.stop();
  }

  @Test
  public void testFullChannel() throws Exception {
    HttpResponse response = putWithEncoding("UTF-8", 150).response;
    Assert.assertEquals(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
            response.getStatusLine().getStatusCode());
    SourceCounter sc = (SourceCounter) Whitebox.getInternalState(httpSource, "sourceCounter");
    Assert.assertEquals(1, sc.getChannelWriteFail());
  }

  @Test
  public void testFail() throws Exception {
    HTTPSourceHandler handler = field("handler").ofType(HTTPSourceHandler.class)
            .in(httpSource).get();
    //Cause an exception in the source - this is equivalent to any exception
    //thrown by the handler since the handler is called inside a try-catch
    field("handler").ofType(HTTPSourceHandler.class).in(httpSource).set(null);
    HttpResponse response = putWithEncoding("UTF-8", 1).response;
    Assert.assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
            response.getStatusLine().getStatusCode());
    //Set the original handler back so tests don't fail after this runs.
    field("handler").ofType(HTTPSourceHandler.class).in(httpSource).set(handler);
  }

  @Test
  public void testMBeans() throws Exception {
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName = new ObjectName("org.eclipse.jetty.*:*");
    Set<ObjectInstance> queryMBeans = mbeanServer.queryMBeans(objectName, null);
    Assert.assertTrue(queryMBeans.size() > 0);
  }

  @Test
  public void testHandlerThrowingException() throws Exception {
    //This will cause the handler to throw an
    //UnsupportedCharsetException.
    HttpResponse response = putWithEncoding("ISO-8859-1", 150).response;
    Assert.assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
            response.getStatusLine().getStatusCode());
  }

  private Set<ObjectInstance> findMBeans(String name, String attribute, int value)
      throws MalformedObjectNameException {
    MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    ObjectName objectName = new ObjectName(name);
    QueryExp q = Query.eq(Query.attr(attribute), Query.value(value));
    return mbeanServer.queryMBeans(objectName, q);
  }

  private ResultWrapper putWithEncoding(String encoding, int n) throws Exception {
    Type listType = new TypeToken<List<JSONEvent>>() {}.getType();
    List<JSONEvent> events = new ArrayList<JSONEvent>();
    Random rand = new Random();
    for (int i = 0; i < n; i++) {
      Map<String, String> input = Maps.newHashMap();
      for (int j = 0; j < 10; j++) {
        input.put(String.valueOf(i) + String.valueOf(j), String.valueOf(i));
      }
      JSONEvent e = new JSONEvent();
      e.setHeaders(input);
      e.setBody(String.valueOf(rand.nextGaussian()).getBytes(encoding));
      events.add(e);
    }
    Gson gson = new Gson();
    String json = gson.toJson(events, listType);
    StringEntity input = new StringEntity(json);
    input.setContentType("application/json; charset=" + encoding);
    postRequest.setEntity(input);
    HttpResponse resp = httpClient.execute(postRequest);
    return new ResultWrapper(resp, events);
  }

  @Test
  public void testHttps() throws Exception {
    doTestHttps(null, httpsPort, httpsChannel);
  }

  @Test (expected = javax.net.ssl.SSLHandshakeException.class)
  public void testHttpsSSLv3() throws Exception {
    doTestHttps("SSLv3", httpsPort, httpsChannel);
  }

  @Test
  public void testHttpsGlobalKeystore() throws Exception {
    doTestHttps(null, httpsGlobalKeystorePort, httpsGlobalKeystoreChannel);
  }

  private void doTestHttps(String protocol, int port, Channel channel) throws Exception {
    Type listType = new TypeToken<List<JSONEvent>>() {
    }.getType();
    List<JSONEvent> events = new ArrayList<JSONEvent>();
    Random rand = new Random();
    for (int i = 0; i < 10; i++) {
      Map<String, String> input = Maps.newHashMap();
      for (int j = 0; j < 10; j++) {
        input.put(String.valueOf(i) + String.valueOf(j), String.valueOf(i));
      }
      input.put("MsgNum", String.valueOf(i));
      JSONEvent e = new JSONEvent();
      e.setHeaders(input);
      e.setBody(String.valueOf(rand.nextGaussian()).getBytes("UTF-8"));
      events.add(e);
    }
    Gson gson = new Gson();
    String json = gson.toJson(events, listType);
    HttpsURLConnection httpsURLConnection = null;
    Transaction transaction = null;
    try {
      TrustManager[] trustAllCerts = {
        new X509TrustManager() {
          @Override
          public void checkClientTrusted(java.security.cert.X509Certificate[] x509Certificates,
                                         String s) throws CertificateException {
            // noop
          }

          @Override
          public void checkServerTrusted(java.security.cert.X509Certificate[] x509Certificates,
                                         String s) throws CertificateException {
            // noop
          }

          public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return null;
          }
        }
      };

      SSLContext sc = null;
      javax.net.ssl.SSLSocketFactory factory = null;
      if (System.getProperty("java.vendor").contains("IBM")) {
        sc = SSLContext.getInstance("SSL_TLS");
      } else {
        sc = SSLContext.getInstance("SSL");
      }

      HostnameVerifier hv = new HostnameVerifier() {
        public boolean verify(String arg0, SSLSession arg1) {
          return true;
        }
      };
      sc.init(null, trustAllCerts, new SecureRandom());

      if (protocol != null) {
        factory = new DisabledProtocolsSocketFactory(sc.getSocketFactory(), protocol);
      } else {
        factory = sc.getSocketFactory();
      }
      HttpsURLConnection.setDefaultSSLSocketFactory(factory);
      HttpsURLConnection.setDefaultHostnameVerifier(NoopHostnameVerifier.INSTANCE);
      URL sslUrl = new URL("https://0.0.0.0:" + port);
      httpsURLConnection = (HttpsURLConnection) sslUrl.openConnection();
      httpsURLConnection.setDoInput(true);
      httpsURLConnection.setDoOutput(true);
      httpsURLConnection.setRequestMethod("POST");
      httpsURLConnection.getOutputStream().write(json.getBytes());

      int statusCode = httpsURLConnection.getResponseCode();
      Assert.assertEquals(200, statusCode);

      transaction = channel.getTransaction();
      transaction.begin();
      for (int i = 0; i < 10; i++) {
        Event e = channel.take();
        Assert.assertNotNull(e);
        Assert.assertEquals(String.valueOf(i), e.getHeaders().get("MsgNum"));
      }

    } finally {
      if (transaction != null) {
        transaction.commit();
        transaction.close();
      }
      httpsURLConnection.disconnect();
    }
  }

  @Test
  public void testHttpsSourceNonHttpsClient() throws Exception {
    Type listType = new TypeToken<List<JSONEvent>>() {
    }.getType();
    List<JSONEvent> events = new ArrayList<JSONEvent>();
    Random rand = new Random();
    for (int i = 0; i < 10; i++) {
      Map<String, String> input = Maps.newHashMap();
      for (int j = 0; j < 10; j++) {
        input.put(String.valueOf(i) + String.valueOf(j), String.valueOf(i));
      }
      input.put("MsgNum", String.valueOf(i));
      JSONEvent e = new JSONEvent();
      e.setHeaders(input);
      e.setBody(String.valueOf(rand.nextGaussian()).getBytes("UTF-8"));
      events.add(e);
    }
    Gson gson = new Gson();
    String json = gson.toJson(events, listType);
    HttpURLConnection httpURLConnection = null;
    try {
      URL url = new URL("http://0.0.0.0:" + httpsPort);
      httpURLConnection = (HttpURLConnection) url.openConnection();
      httpURLConnection.setDoInput(true);
      httpURLConnection.setDoOutput(true);
      httpURLConnection.setRequestMethod("POST");
      httpURLConnection.getOutputStream().write(json.getBytes());
      httpURLConnection.getResponseCode();

      Assert.fail("HTTP Client cannot connect to HTTPS source");
    } catch (Exception exception) {
      Assert.assertTrue("Exception expected", true);
    } finally {
      httpURLConnection.disconnect();
    }
  }

  private void takeWithEncoding(String encoding, int n, List<JSONEvent> events) throws Exception {
    Transaction tx = httpChannel.getTransaction();
    tx.begin();
    Event e = null;
    int i = 0;
    while (true) {
      e = httpChannel.take();
      if (e == null) {
        break;
      }
      Event current = events.get(i++);
      Assert.assertEquals(new String(current.getBody(), encoding),
                          new String(e.getBody(), encoding));
      Assert.assertEquals(current.getHeaders(), e.getHeaders());
    }
    Assert.assertEquals(n, events.size());
    tx.commit();
    tx.close();
  }


  private void testBatchWithVariousEncoding(String encoding) throws Exception {
    testBatchWithVariousEncoding(encoding, 50);
  }
  private void testBatchWithVariousEncoding(String encoding, int n)
          throws Exception {
    List<JSONEvent> events = putWithEncoding(encoding, n).events;
    takeWithEncoding(encoding, n, events);
  }

  private class ResultWrapper {
    public final HttpResponse response;
    public final List<JSONEvent> events;

    public ResultWrapper(HttpResponse resp, List<JSONEvent> events) {
      this.response = resp;
      this.events = events;
    }
  }

  private class DisabledProtocolsSocketFactory extends javax.net.ssl.SSLSocketFactory {

    private final javax.net.ssl.SSLSocketFactory socketFactory;
    private final String[] protocols;

    DisabledProtocolsSocketFactory(javax.net.ssl.SSLSocketFactory factory, String protocol) {
      this.socketFactory = factory;
      protocols = new String[1];
      protocols[0] = protocol;
    }

    @Override
    public String[] getDefaultCipherSuites() {
      return socketFactory.getDefaultCipherSuites();
    }

    @Override
    public String[] getSupportedCipherSuites() {
      return socketFactory.getSupportedCipherSuites();
    }

    @Override
    public Socket createSocket(Socket socket, String s, int i, boolean b) throws IOException {
      SSLSocket sc = (SSLSocket) socketFactory.createSocket(socket, s, i, b);
      sc.setEnabledProtocols(protocols);
      return sc;
    }

    @Override
    public Socket createSocket(String s, int i) throws IOException, UnknownHostException {
      SSLSocket sc = (SSLSocket) socketFactory.createSocket(s, i);
      sc.setEnabledProtocols(protocols);
      return sc;
    }

    @Override
    public Socket createSocket(String s, int i, InetAddress inetAddress, int i2)
        throws IOException, UnknownHostException {
      SSLSocket sc = (SSLSocket) socketFactory.createSocket(s, i, inetAddress, i2);
      sc.setEnabledProtocols(protocols);
      return sc;
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int i) throws IOException {
      SSLSocket sc = (SSLSocket) socketFactory.createSocket(inetAddress, i);
      sc.setEnabledProtocols(protocols);
      return sc;
    }

    @Override
    public Socket createSocket(InetAddress inetAddress, int i,
                               InetAddress inetAddress2, int i2) throws IOException {
      SSLSocket sc = (SSLSocket) socketFactory.createSocket(inetAddress, i,
                                                            inetAddress2, i2);
      sc.setEnabledProtocols(protocols);
      return sc;
    }
  }
}
