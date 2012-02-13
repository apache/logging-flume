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
import org.apache.flume.channel.jdbc.JdbcChannelException;

public class PersistableEvent implements Event {

  private long eventId;
  private final String channel;
  private byte[] basePayload;
  private byte[] spillPayload;
  private List<HeaderEntry> headers;

  public PersistableEvent(String channel, Event event) {
    this.channel = channel;

    byte[] givenPayload = event.getBody();
    if (givenPayload.length < ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD) {
      basePayload = Arrays.copyOf(givenPayload, givenPayload.length);
      spillPayload = null;
    } else {
      basePayload = Arrays.copyOfRange(givenPayload, 0,
          ConfigurationConstants.PAYLOAD_LENGTH_THRESHOLD);
      spillPayload = Arrays.copyOfRange(givenPayload,
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

  private PersistableEvent(long eventId, String channel, byte[] basePayload,
      byte[] spillPayload, List<HeaderEntry> headers) {
    this.eventId = eventId;
    this.channel = channel;
    this.basePayload = basePayload;
    this.spillPayload = spillPayload;
    this.headers = headers;
  }

  public String getChannelName() {
    return channel;
  }

  public byte[] getBasePayload() {
    return this.basePayload;
  }

  public byte[] getSpillPayload() {
    return this.spillPayload;
  }

  protected void setEventId(long eventId) {
    this.eventId = eventId;
  }

  protected long getEventId() {
    return this.eventId;
  }

  public List<HeaderEntry> getHeaderEntries() {
    return headers;
  }

  protected static class HeaderEntry {

    private long headerId = -1L;
    private SpillableString name;
    private SpillableString value;

    public HeaderEntry(String name, String value) {
      this.name = new SpillableString(name,
          ConfigurationConstants.HEADER_NAME_LENGTH_THRESHOLD);
      this.value = new SpillableString(value,
          ConfigurationConstants.HEADER_VALUE_LENGTH_THRESHOLD);
    }

    private HeaderEntry(long headerId, String baseName, String spillName,
        String baseValue, String spillValue) {
      this.headerId = headerId;
      this.name = new SpillableString(baseName, spillName);
      this.value = new SpillableString(baseValue, spillValue);
    }

    public String getNameString() {
      return name.getString();
    }

    public SpillableString getName() {
      return name;
    }

    public String getValueString() {
      return value.getString();
    }

    public SpillableString getValue() {
      return value;
    }

    protected void setId(long headerId) {
      this.headerId = headerId;
    }

    public long getId() {
      return headerId;
    }

  }

  protected static class SpillableString {

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

    private SpillableString(String base, String spill) {
      this.base = base;
      this.spill = spill;
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

    public boolean hasSpill() {
      return spill != null;
    }
  }

  @Override
  public void setHeaders(Map<String, String> headers) {
    throw new UnsupportedOperationException("Cannot update headers of "
        + "persistable event");
  }

  @Override
  public byte[] getBody() {
    byte[] result = null;
    if (spillPayload == null) {
      result = Arrays.copyOf(basePayload, basePayload.length);
    } else {
      result = new byte[basePayload.length + spillPayload.length];
      System.arraycopy(basePayload, 0, result, 0, basePayload.length);
      System.arraycopy(spillPayload, 0, result,
          basePayload.length, spillPayload.length);
    }

    return result;
  }

  @Override
  public void setBody(byte[] body) {
    throw new UnsupportedOperationException("Cannot update payload of "
        + "persistable event");
  }

  @Override
  public Map<String, String> getHeaders() {
    Map<String, String> headerMap = null;
    if (headers != null) {
      headerMap =  new HashMap<String, String>();
      for (HeaderEntry entry :  headers) {
        headerMap.put(entry.getNameString(), entry.getValueString());
      }
    }

    return headerMap;
  }

  public static class Builder {

    private long bEventId;
    private String bChannelName;
    private byte[] bBasePayload;
    private byte[] bSpillPayload;
    private Map<Long, HeaderPart> bHeaderParts;

    public Builder(String channelName, long eventId) {
      bChannelName = channelName;
      bEventId = eventId;
    }

    public Builder setEventId(long eventId) {
      bEventId = eventId;
      return this;
    }

    public Builder setChannel(String channel) {
      bChannelName = channel;
      return this;
    }

    public Builder setBasePayload(byte[] basePayload) {
      bBasePayload = basePayload;
      return this;
    }

    public Builder setSpillPayload(byte[] spillPayload) {
      bSpillPayload = spillPayload;
      return this;
    }

    public Builder setHeader(long headerId, String baseName, String baseValue) {
      if (bHeaderParts == null) {
        bHeaderParts = new HashMap<Long, HeaderPart>();
      }
      HeaderPart hp = new HeaderPart(baseName, baseValue);
      if (bHeaderParts.put(headerId, hp) != null) {
        throw new JdbcChannelException("Duplicate header found: "
            + "headerId: " + headerId + ", baseName: " + baseName + ", "
            + "baseValue: " + baseValue);
      }

      return this;
    }

    public Builder setHeaderNameSpill(long headerId, String nameSpill) {
      HeaderPart hp = bHeaderParts.get(headerId);
      if (hp == null) {
        throw new JdbcChannelException("Header not found for spill: "
            + headerId);
      }

      hp.setSpillName(nameSpill);

      return this;
    }

    public Builder setHeaderValueSpill(long headerId, String valueSpill) {
      HeaderPart hp = bHeaderParts.get(headerId);
      if (hp == null) {
        throw new JdbcChannelException("Header not found for spill: "
            + headerId);
      }

      hp.setSpillValue(valueSpill);

      return this;
    }

    public PersistableEvent build() {
      List<HeaderEntry> bHeaders = new ArrayList<HeaderEntry>();
      if (bHeaderParts != null) {
        for (long headerId : bHeaderParts.keySet()) {
          HeaderPart part = bHeaderParts.get(headerId);
          bHeaders.add(part.getEntry(headerId));
        }
      }

      PersistableEvent pe = new PersistableEvent(bEventId, bChannelName,
          bBasePayload, bSpillPayload, bHeaders);

      bEventId = 0L;
      bChannelName = null;
      bBasePayload = null;
      bSpillPayload = null;
      bHeaderParts = null;

      return pe;
    }
  }

  @SuppressWarnings("unused")
  private static class HeaderPart {
    private final String hBaseName;
    private final String hBaseValue;
    private String hSpillName;
    private String hSpillValue;

    HeaderPart(String baseName, String baseValue) {
      hBaseName = baseName;
      hBaseValue = baseValue;
    }

    String getBaseName() {
      return hBaseName;
    }

    String getBaseValue() {
      return hBaseValue;
    }

    String getSpillName() {
      return hSpillName;
    }

    String getSpillValue() {
      return hSpillValue;
    }

    void setSpillName(String spillName) {
      hSpillName = spillName;
    }

    void setSpillValue(String spillValue) {
      hSpillValue = spillValue;
    }

    HeaderEntry getEntry(long headerId) {
      return new HeaderEntry(headerId, hBaseName,
          hSpillName, hBaseValue, hSpillValue);
    }
  }

}
