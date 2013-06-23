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
package org.apache.flume.channel.file;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Event;

/**
 * Persistable wrapper for Event
 */
class FlumeEvent implements Event, Writable {

  private static final byte EVENT_MAP_TEXT_WRITABLE_ID = Byte.valueOf(Integer.valueOf(-116).byteValue());

  private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY =
      new ThreadLocal<CharsetEncoder>() {
    @Override
    protected CharsetEncoder initialValue() {
      return Charset.forName("UTF-8").newEncoder().
          onMalformedInput(CodingErrorAction.REPLACE).
          onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
  };

  private static ThreadLocal<CharsetDecoder> DECODER_FACTORY =
      new ThreadLocal<CharsetDecoder>() {
    @Override
    protected CharsetDecoder initialValue() {
      return Charset.forName("UTF-8").newDecoder().
          onMalformedInput(CodingErrorAction.REPLACE).
          onUnmappableCharacter(CodingErrorAction.REPLACE);
    }
  };

  private Map<String, String> headers;
  private byte[] body;

  private FlumeEvent() {
    this(null, null);
  }
  FlumeEvent(Map<String, String> headers, byte[] body) {
    this.headers = headers;
    this.body = body;
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
  public void write(DataOutput out) throws IOException {

    out.writeByte(0);

    Map<String,String> writeHeaders = getHeaders();
    if (null != writeHeaders) {
      out.writeInt(headers.size());

      CharsetEncoder encoder = ENCODER_FACTORY.get();

      for (String key : headers.keySet()) {
        out.writeByte(EVENT_MAP_TEXT_WRITABLE_ID);
        ByteBuffer keyBytes = encoder.encode(CharBuffer.wrap(key.toCharArray()));
        int keyLength = keyBytes.limit();
        WritableUtils.writeVInt(out, keyLength);
        out.write(keyBytes.array(), 0, keyLength);

        String value = headers.get(key);
        out.write(EVENT_MAP_TEXT_WRITABLE_ID);
        ByteBuffer valueBytes = encoder.encode(CharBuffer.wrap(value.toCharArray()));
        int valueLength = valueBytes.limit();
        WritableUtils.writeVInt(out, valueLength );
        out.write(valueBytes.array(), 0, valueLength);
      }
    }
    else {
      out.writeInt( 0 );
    }

    byte[] body = getBody();
    if(body == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(body.length);
      out.write(body);
    }
  }


  @Override
  public void readFields(DataInput in) throws IOException {

    // newClasses from AbstractMapWritable in Hadoop Common
    byte newClasses = in.readByte();

    // skip over newClasses since only Text is used
    for (byte i = 0; i < newClasses; i++) {
      in.readByte();
      in.readUTF();
    }

    Map<String,String> newHeaders = new HashMap<String,String>();

    int numEntries = in.readInt();

    CharsetDecoder decoder = DECODER_FACTORY.get();

    for (int i = 0; i < numEntries; i++) {

      byte keyClassId = in.readByte();
      assert (keyClassId == EVENT_MAP_TEXT_WRITABLE_ID);
      int keyLength = WritableUtils.readVInt(in);
      byte[] keyBytes = new byte[ keyLength ];

      in.readFully( keyBytes, 0, keyLength );
      String key = decoder.decode( ByteBuffer.wrap(keyBytes) ).toString();

      byte valueClassId = in.readByte();
      assert (valueClassId == EVENT_MAP_TEXT_WRITABLE_ID);
      int valueLength = WritableUtils.readVInt(in);
      byte[] valueBytes = new byte[ valueLength ];

      in.readFully(valueBytes, 0, valueLength);
      String value = decoder.decode(ByteBuffer.wrap(valueBytes)).toString();

      newHeaders.put(key,  value);
    }

    setHeaders(newHeaders);

    byte[] body = null;
    int bodyLength = in.readInt();
    if(bodyLength != -1) {
      body = new byte[bodyLength];
      in.readFully(body);
    }
    setBody(body);
  }
  static FlumeEvent from(DataInput in) throws IOException {
    FlumeEvent event = new FlumeEvent();
    event.readFields(in);
    return event;
  }
}
