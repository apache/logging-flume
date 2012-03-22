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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.HexDump;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleEvent implements Event {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(SimpleEvent.class);
  private static final String EOL = System.getProperty("line.separator", "\n");
  private static final String HEXDUMP_OFFSET = "00000000";
  private Map<String, String> headers;
  private byte[] body;

  public SimpleEvent() {
    headers = new HashMap<String, String>();
    body = null;
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
    this.body = body;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    if(body == null) {
      buffer.append("null");
    } else {
      byte[] data = Arrays.copyOf(body, Math.min(body.length, 16));
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      try {
        HexDump.dump(data, 0, out, 0);
        String hexDump = new String(out.toByteArray());
        // remove offset since it's not relevant for such a small dataset
        if(hexDump.startsWith(HEXDUMP_OFFSET)) {
          hexDump = hexDump.substring(HEXDUMP_OFFSET.length());
        }
        buffer.append(hexDump);
      } catch (Exception e) {
       if(LOGGER.isInfoEnabled()) {
         LOGGER.info("Exception while dumping event", e);
       }
       buffer.append("...Exception while dumping: " + e.getMessage());
      }
      String result = buffer.toString();
      if(result.endsWith(EOL) && buffer.length() > EOL.length()) {
        result = buffer.delete(buffer.length() - EOL.length(), buffer.length()).toString();
      }
    }
    return "{ headers:" + headers + " body:" + buffer + " }";
  }

}
