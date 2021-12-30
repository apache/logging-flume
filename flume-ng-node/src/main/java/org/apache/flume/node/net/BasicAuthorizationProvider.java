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

import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Provides the Basic Authorization header to a request.
 */
public class BasicAuthorizationProvider implements AuthorizationProvider {

  private static final Base64.Encoder encoder = Base64.getEncoder();

  private String authString = null;

  public BasicAuthorizationProvider(String userName, String password) {
    if (userName != null && password != null) {
      String toEncode = userName + ":" + password;
      authString = "Basic " + encoder.encodeToString(toEncode.getBytes(StandardCharsets.UTF_8));
    }
  }

  @Override
  public void addAuthorization(URLConnection urlConnection) {
    if (authString != null) {
      urlConnection.setRequestProperty("Authorization", authString);
    }
  }
}
