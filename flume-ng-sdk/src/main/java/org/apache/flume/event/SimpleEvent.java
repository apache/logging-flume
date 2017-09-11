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

package org.apache.flume.event;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;

public class SimpleEvent implements Event {

  private Map<String, String> headers;
  private byte[] body;

  public SimpleEvent() {
    headers = new HashMap<String, String>();
    body = new byte[0];
  }

  @Override
  public Map<String, String> getHeaders() {
    return headers;
  }

  @Override
  public void setHeaders(Map<String, String> headers) {
    this.headers = headers;
  }

  @Override
  public byte[] getBody() {
    return body;
  }

  @Override
  public void setBody(byte[] body) {
    if (body == null) {
      body = new byte[0];
    }
    this.body = body;
  }

  @Override
  public String toString() {
    Integer bodyLen = null;
    if (body != null) bodyLen = body.length;
    return "[Event headers = " + headers + ", body.length = " + bodyLen + " ]";
  }

  public void notifySource() {}

  public void commit() {}
}
