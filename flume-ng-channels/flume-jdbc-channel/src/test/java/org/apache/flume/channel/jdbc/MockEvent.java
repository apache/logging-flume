/**
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
package org.apache.flume.channel.jdbc;

import java.util.Map;

import org.apache.flume.Event;

public class MockEvent implements Event {

  private final byte[] payload;
  private final Map<String, String> headers;
  private final String channel;

  public MockEvent(byte[] payload, Map<String, String> headers, String channel)
  {
    this.payload = payload;
    this.headers = headers;
    this.channel = channel;
  }

  @Override
  public Map<String, String> getHeaders() {
    return headers;
  }

  @Override
  public void setHeaders(Map<String, String> headers) {

  }

  @Override
  public byte[] getBody() {
    return payload;
  }

  @Override
  public void setBody(byte[] body) {

  }

  public String getChannel() {
    return channel;
  }

}
