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

package org.apache.flume.event;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import org.apache.flume.Event;

/**
 *
 */
public class JSONEvent implements Event{
  private Map<String, String> headers;
  private String body;
  private transient String charset = "UTF-8";

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
    if(body != null) {
      try {
        return body.getBytes(charset);
      } catch (UnsupportedEncodingException ex) {
        //Should never happen
        return null;
      }
    } else {
      return new byte[0];
    }

  }

  @Override
  public void setBody(byte[] body) {
    if(body != null) {
      this.body = new String(body);
    } else {
      this.body = "";
    }
  }

  public void setCharset(String charset) {
    this.charset = charset;
  }

}
