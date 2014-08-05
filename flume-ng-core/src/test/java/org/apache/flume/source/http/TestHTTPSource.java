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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import junit.framework.Assert;
import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.JSONEvent;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpTrace;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.net.ssl.*;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.URL;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.fest.reflect.core.Reflection.field;

/**
 *
 */
public class TestHTTPSource {

  private static HTTPSource source;
  private static HTTPSource httpsSource;
//  private static Channel httpsChannel;

  private static Channel channel;
  private static int selectedPort;
  private static int sslPort;
  DefaultHttpClient httpClient;
  HttpPost postRequest;

  private static int findFreePort() throws IOException {
    ServerSocket socket = new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    selectedPort = findFreePort();

    source = new HTTPSource();
    channel = new MemoryChannel();

    httpsSource = new HTTPSource();
    httpsSource.setName("HTTPS Source");

    Context ctx = new Context();
    ctx.put("capacity", "100");
    Configurables.configure(channel, ctx);

    List<Channel> channels = new ArrayList<Channel>(1);
    channels.add(channel);

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(channels);

    source.setChannelProcessor(new ChannelProcessor(rcs));

    channel.start();

    httpsSource.setChannelProcessor(new ChannelProcessor(rcs));

    // HTTP context
    Context context = new Context();

    context.put("port", String.valueOf(selectedPort));
    context.put("host", "0.0.0.0");

    // SSL context props
    Context sslContext = new Context();
    sslContext.put(HTTPSourceConfigurationConstants.SSL_ENABLED, "true");
    sslPort = findFreePort();
    sslContext.put(HTTPSourceConfigurationConstants.CONFIG_PORT,
      String.valueOf(sslPort));
    sslContext.put(HTTPSourceConfigurationConstants.SSL_KEYSTORE_PASSWORD, "password");
    sslContext.put(HTTPSourceConfigurationConstants.SSL_KEYSTORE, "src/test/resources/jettykeystore");

    Configurables.configure(source, context);
    Configurables.configure(httpsSource, sslContext);
    source.start();
    httpsSource.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    source.stop();
    channel.stop();
    httpsSource.stop();
  }

  @Before
  public void setUp() {
    httpClient = new DefaultHttpClient();
    postRequest = new HttpPost("http://0.0.0.0:" + selectedPort);
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
    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e = channel.take();
    Assert.assertNotNull(e);
    Assert.assertEquals("b", e.getHeaders().get("a"));
    Assert.assertEquals("random_body", new String(e.getBody(), "UTF-8"));

    e = channel.take();
    Assert.assertNotNull(e);
    Assert.assertEquals("f", e.getHeaders().get("e"));
    Assert.assertEquals("random_body2", new String(e.getBody(), "UTF-8"));
    tx.commit();
    tx.close();
  }

  @Test
  public void testTrace() throws Exception {
    doTestForbidden(new HttpTrace("http://0.0.0.0:" + selectedPort));
  }

  @Test
  public void testOptions() throws Exception {
    doTestForbidden(new HttpOptions("http://0.0.0.0:" + selectedPort));
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
    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e = channel.take();
    Assert.assertNotNull(e);
    Assert.assertEquals("b", e.getHeaders().get("a"));
    Assert.assertEquals("random_body", new String(e.getBody(), "UTF-16"));

    e = channel.take();
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

  }

  @Test
  public void testBigBatchDeserializarionUTF8() throws Exception {
    testBatchWithVariousEncoding("UTF-8");
  }

  @Test
  public void testBigBatchDeserializarionUTF16() throws Exception {
    testBatchWithVariousEncoding("UTF-16");
  }

  @Test
  public void testBigBatchDeserializarionUTF32() throws Exception {
    testBatchWithVariousEncoding("UTF-32");
  }
  @Test
  public void testSingleEvent() throws Exception {
    StringEntity input = new StringEntity("[{\"headers\" : {\"a\": \"b\"},\"body\":"
            + " \"random_body\"}]");
    input.setContentType("application/json");
    postRequest.setEntity(input);

    httpClient.execute(postRequest);
    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e = channel.take();
    Assert.assertNotNull(e);
    Assert.assertEquals("b", e.getHeaders().get("a"));
    Assert.assertEquals("random_body", new String(e.getBody(),"UTF-8"));
    tx.commit();
    tx.close();
  }

  @Test
  public void testFullChannel() throws Exception {
    HttpResponse response = putWithEncoding("UTF-8", 150).response;
    Assert.assertEquals(HttpServletResponse.SC_SERVICE_UNAVAILABLE,
            response.getStatusLine().getStatusCode());
  }

  @Test
  public void testFail() throws Exception {
    HTTPSourceHandler handler = field("handler").ofType(HTTPSourceHandler.class)
            .in(source).get();
    //Cause an exception in the source - this is equivalent to any exception
    //thrown by the handler since the handler is called inside a try-catch
    field("handler").ofType(HTTPSourceHandler.class).in(source).set(null);
    HttpResponse response = putWithEncoding("UTF-8", 1).response;
    Assert.assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
            response.getStatusLine().getStatusCode());
    //Set the original handler back so tests don't fail after this runs.
    field("handler").ofType(HTTPSourceHandler.class).in(source).set(handler);
  }

  @Test
  public void testHandlerThrowingException() throws Exception {
    //This will cause the handler to throw an
    //UnsupportedCharsetException.
    HttpResponse response = putWithEncoding("ISO-8859-1", 150).response;
    Assert.assertEquals(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
            response.getStatusLine().getStatusCode());
  }


  private ResultWrapper putWithEncoding(String encoding, int n)
          throws Exception{
    Type listType = new TypeToken<List<JSONEvent>>() {
    }.getType();
    List<JSONEvent> events = Lists.newArrayList();
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
    Type listType = new TypeToken<List<JSONEvent>>() {
    }.getType();
    List<JSONEvent> events = Lists.newArrayList();
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
    try {
      TrustManager[] trustAllCerts = {new X509TrustManager() {
        @Override
        public void checkClientTrusted(
          java.security.cert.X509Certificate[] x509Certificates, String s)
          throws CertificateException {
          // noop
        }

        @Override
        public void checkServerTrusted(
          java.security.cert.X509Certificate[] x509Certificates, String s)
          throws CertificateException {
          // noop
        }

        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
          return null;
        }
      }};

      SSLContext sc = null;
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
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
      HttpsURLConnection.setDefaultHostnameVerifier(
        SSLSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);
      URL sslUrl = new URL("https://0.0.0.0:" + sslPort);
      httpsURLConnection = (HttpsURLConnection) sslUrl.openConnection();
      httpsURLConnection.setDoInput(true);
      httpsURLConnection.setDoOutput(true);
      httpsURLConnection.setRequestMethod("POST");
      httpsURLConnection.getOutputStream().write(json.getBytes());

      int statusCode = httpsURLConnection.getResponseCode();
      Assert.assertEquals(200, statusCode);

      Transaction transaction = channel.getTransaction();
      transaction.begin();
      for(int i = 0; i < 10; i++) {
        Event e = channel.take();
        Assert.assertNotNull(e);
        Assert.assertEquals(String.valueOf(i), e.getHeaders().get("MsgNum"));
      }

    transaction.commit();
    transaction.close();
    } catch (Exception exception) {
      Assert.fail("Exception not expected");
    } finally {
      httpsURLConnection.disconnect();
    }
  }

  @Test
  public void testHttpsSourceNonHttpsClient() throws Exception {
    Type listType = new TypeToken<List<JSONEvent>>() {
    }.getType();
    List<JSONEvent> events = Lists.newArrayList();
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
        URL url = new URL("http://0.0.0.0:" + sslPort);
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

  private void takeWithEncoding(String encoding, int n, List<JSONEvent> events)
          throws Exception{
    Transaction tx = channel.getTransaction();
    tx.begin();
    Event e = null;
    int i = 0;
    while (true) {
      e = channel.take();
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
    public ResultWrapper(HttpResponse resp, List<JSONEvent> events){
      this.response = resp;
      this.events = events;
    }
  }
}
