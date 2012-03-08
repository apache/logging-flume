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
package org.apache.flume.channel;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;

public class MockEvent implements Event {

  private Map<String, String> headers;

  private byte[] body;

  public MockEvent() {
    this(new HashMap<String, String>(), new byte[0]);
  }

  public MockEvent(Map<String, String> headers, byte[] body) {
    this.headers = new HashMap<String, String>();
    this.headers.putAll(headers);

    this.body = new byte[body.length];
    System.arraycopy(body, 0, this.body, 0, body.length);
  }

  @Override
  public Map<String, String> getHeaders() {
    return headers;
  }

  @Override
  public void setHeaders(Map<String, String> headers) {
    this.headers = new HashMap<String, String>();
    this.headers.putAll(headers);
  }

  @Override
  public byte[] getBody() {
    return body;
  }

  @Override
  public void setBody(byte[] body) {
    this.body = new byte[body.length];
    System.arraycopy(body, 0, this.body, 0, body.length);
  }
}
