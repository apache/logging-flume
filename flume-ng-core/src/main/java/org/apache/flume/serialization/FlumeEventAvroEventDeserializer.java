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
package org.apache.flume.serialization;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A deserializer that parses Avro container files, generating one Flume event
 * per record in the Avro file, and storing binary avro-encoded records in
 * the Flume event body.
 * If a header is available, restore the header correctly by separating if from the payload
 */
public class FlumeEventAvroEventDeserializer extends AbstractAvroEventDeserializer {

  private static final Logger logger =
          LoggerFactory.getLogger(FlumeEventAvroEventDeserializer.class);

  private FlumeEventAvroEventDeserializer(Context context, ResettableInputStream ris) {
    super(context, ris);
  }

  @Override
  public Event readEvent() throws IOException {
    Event event = super.readEvent();

    if (null != event) {

      byte[] bodyBinary = event.getBody();

      boolean size = schema.getFields().size() == 2;

      boolean hasHeaders = false;
      int headersPosition = -1;

      boolean hasBody = false;
      int bodyPosition = -1;

      try {
        Schema.Field f = schema.getField("headers");
        headersPosition = f.pos();
        hasHeaders = true;
      } catch (Exception ex) {
        logger.debug("No headers found");
      }

      try {
        Schema.Field f = schema.getField("body");
        bodyPosition = f.pos();
        hasBody = true;
      } catch (Exception ex) {
        logger.debug("No body found");
      }

      if (size && hasBody && hasHeaders) {
        ByteBuffer body = (ByteBuffer) record.get(bodyPosition);
        bodyBinary = body.array();
      }

      event.setBody(bodyBinary);

      if (size && hasBody && hasHeaders) {
        HashMap<String, String> header = (HashMap<String, String>) record.get(headersPosition);
        Iterator it = header.entrySet().iterator();
        while (it.hasNext()) {
          Map.Entry<Utf8, Utf8> pair = (Map.Entry) it.next();
          event.getHeaders().put(pair.getKey().toString(), pair.getValue().toString());
        }
      }
    }

    return event;
  }

  public static class Builder implements EventDeserializer.Builder {

    @Override
    public EventDeserializer build(Context context, ResettableInputStream in) {
      if (!(in instanceof RemoteMarkable)) {
        throw new IllegalArgumentException("Cannot use this deserializer " +
                "without a RemoteMarkable input stream");
      }
      FlumeEventAvroEventDeserializer deserializer
              = new FlumeEventAvroEventDeserializer(context, in);
      try {
        deserializer.initialize();
      } catch (Exception e) {
        throw new FlumeException("Cannot instantiate deserializer", e);
      }
      return deserializer;
    }
  }

}