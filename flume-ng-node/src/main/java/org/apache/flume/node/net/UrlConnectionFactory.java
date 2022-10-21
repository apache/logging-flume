/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.apache.flume.node.net;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import javax.net.ssl.HttpsURLConnection;

/**
 * Constructs an HTTPURLConnection.
 */
public class UrlConnectionFactory {

  private static int DEFAULT_TIMEOUT = 60000;
  private static int connectTimeoutMillis = DEFAULT_TIMEOUT;
  private static int readTimeoutMillis = DEFAULT_TIMEOUT;
  private static final String XML = "application/xml";
  private static final String YAML = "application/yaml";
  private static final String JSON = "application/json";
  private static final String PROPERTIES = "text/x-java-properties";
  private static final String TEXT = "text/plain";
  public static final String HTTP = "http";
  public static final String HTTPS = "https";

  public static HttpURLConnection createConnection(URL url,
      AuthorizationProvider authorizationProvider, long lastModifiedMillis, boolean verifyHost)
      throws IOException {
    final HttpURLConnection urlConnection = (HttpURLConnection) url.openConnection();
    if (HTTPS.equals(url.getProtocol()) && !verifyHost) {
      ((HttpsURLConnection) urlConnection).setHostnameVerifier(LaxHostnameVerifier.INSTANCE);
    }
    if (authorizationProvider != null) {
      authorizationProvider.addAuthorization(urlConnection);
    }
    urlConnection.setAllowUserInteraction(false);
    urlConnection.setDoOutput(true);
    urlConnection.setDoInput(true);
    urlConnection.setRequestMethod("GET");
    if (connectTimeoutMillis > 0) {
      urlConnection.setConnectTimeout(connectTimeoutMillis);
    }
    if (readTimeoutMillis > 0) {
      urlConnection.setReadTimeout(readTimeoutMillis);
    }
    urlConnection.setRequestProperty("Content-Type", getContentType(url));
    if (lastModifiedMillis > 0) {
      ZonedDateTime zdt = Instant.ofEpochMilli(lastModifiedMillis).atZone(ZoneOffset.UTC);
      String lastModified = DateTimeFormatter.RFC_1123_DATE_TIME.format(zdt);
      urlConnection.setRequestProperty("If-Modified-Since", lastModified);
    }
    return urlConnection;
  }

  public static URLConnection createConnection(URL url) throws IOException {
    return createConnection(url, null, 0, true);
  }

  public static URLConnection createConnection(URL url, AuthorizationProvider authorizationProvider)
      throws IOException {
    URLConnection urlConnection = null;
    if (url.getProtocol().equals(HTTPS) || url.getProtocol().equals(HTTP)) {
      urlConnection = createConnection(url, authorizationProvider, 0, true);
    } else {
      urlConnection = url.openConnection();
    }
    return urlConnection;
  }

  private static String getContentType(URL url) {
    String[] fileParts = url.getFile().split("\\.");
    String type = fileParts[fileParts.length - 1].trim();
    switch (type) {
      case "properties": {
        return PROPERTIES;
      }
      case "json": {
        return JSON;
      }
      case "yaml": case "yml": {
        return YAML;
      }
      case "xml": {
        return XML;
      }
      default: {
        return TEXT;
      }
    }
  }
}
