/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.cloudera.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpServerTestUtils {
  public static final Logger LOG = LoggerFactory
      .getLogger(HttpServerTestUtils.class);

  /**
   * Grab a url's contents. This assumes that grabbed pages are small
   *
   * @param urlString
   * @return
   * @throws IOException
   */
  public static String curl(String urlString) throws IOException {
    URL url = new URL(urlString);
    URLConnection urlConn = url.openConnection();
    urlConn.setDoInput(true);
    urlConn.setUseCaches(false);

    int len = urlConn.getContentLength();
    String type = urlConn.getContentType();
    LOG.info("pulled " + urlString + " [type=" + type + " len=" + len + "]");
    InputStreamReader isr = new InputStreamReader(urlConn.getInputStream());
    BufferedReader br = new BufferedReader(isr);
    StringBuilder sb = new StringBuilder();
    String s;
    while ((s = br.readLine()) != null) {
      sb.append(s);
      sb.append('\n');
    }
    return sb.toString();
  }

  /**
   * Grab a url's http response code. It if fails, it will throw an exception.
   *
   * @param urlString
   * @return
   * @throws IOException
   */
  public static int curlResp(String urlString) throws IOException {
    URL url = new URL(urlString);
    HttpURLConnection urlConn = (HttpURLConnection) url.openConnection();
    urlConn.setDoInput(true);
    urlConn.setUseCaches(false);

    int len = urlConn.getContentLength();
    String type = urlConn.getContentType();
    LOG.info("pulled " + urlString + " [type=" + type + " len=" + len + "]");
    return urlConn.getResponseCode();
  }

}
