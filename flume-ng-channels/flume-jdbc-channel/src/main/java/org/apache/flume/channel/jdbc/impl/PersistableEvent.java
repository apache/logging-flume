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
package org.apache.flume.channel.jdbc.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.channel.jdbc.ConfigurationConstants;

public class PersistableEvent {

  private long eventId;
  private byte[] payload;
  private byte[] spill;
  private List<HeaderEntry> headers;

  public PersistableEvent(Event event) {

    byte[] givenPayload = event.getBody();
    if (givenPayload.length < ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD) {
      payload = Arrays.copyOf(givenPayload, givenPayload.length);
      spill = null;
    } else {
      payload = Arrays.copyOfRange(givenPayload, 0,
          ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD);
      spill = Arrays.copyOfRange(givenPayload,
          ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD, givenPayload.length);
    }

    Map<String, String> headerMap = event.getHeaders();
    if (headerMap != null && headerMap.size() > 0) {
      headers = new ArrayList<HeaderEntry>();
      for (Map.Entry<String, String> entry : headerMap.entrySet()) {
        String name = entry.getKey();
        String value = entry.getValue();
        headers.add(new HeaderEntry(name, value));
      }
    }
  }

  public byte[] getPayload() {
    byte[] result = null;
    if (spill == null) {
      result = Arrays.copyOf(payload, payload.length);
    } else {
      result = new byte[payload.length + spill.length];
      System.arraycopy(payload, 0, result, 0, payload.length);
      System.arraycopy(spill, 0, result, payload.length, spill.length);
    }

    return result;
  }

  public Map<String, String> getHeaders() {
    Map<String, String> headerMap = null;
    if (headers != null) {
      headerMap =  new HashMap<String, String>();
      for (HeaderEntry entry :  headers) {
        headerMap.put(entry.getName(), entry.getValue());
      }
    }

    return headerMap;
  }

  public static class HeaderEntry {

    private SpillableString nameString;
    private SpillableString valueString;

    public HeaderEntry(String name, String value) {
      nameString = new SpillableString(name,
          ConfigurationConstants.HEADER_NAME_LENGTH_THRESHOLD);
      valueString = new SpillableString(value,
          ConfigurationConstants.HEADER_VALUE_LENGTH_THRESHOLD);
    }

    public String getName() {
      return nameString.getString();
    }

    public String getValue() {
      return valueString.getString();
    }
  }

  private static class SpillableString {

    private String base;
    private String spill;

    public SpillableString(String string, int threshold) {
      if (string.getBytes().length < threshold) {
        base = string;
      } else {
        // Identify the maximum character size that will fit in the
        // given threshold
        int currentIndex = threshold / 3; // Assuming 3 byte encoding worst case
        int lastIndex = currentIndex;
        while (true) {
          int length = string.substring(0, currentIndex).getBytes().length;
          if (length < threshold) {
            lastIndex = currentIndex;
            currentIndex++;
          } else {
            break;
          }
        }
        base = string.substring(0, lastIndex);
        spill = string.substring(lastIndex);
      }
    }

    public String getBase() {
      return base;
    }

    public String getSpill() {
      return spill;
    }

    public String getString() {
      if (spill == null) {
        return base;
      }
      return base + spill;
    }
  }
}
