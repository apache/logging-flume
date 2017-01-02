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

import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * A deserializer that parses Avro container files, generating one Flume event
 * per record in the Avro file, and storing binary avro-encoded records in
 * the Flume event body.
 */
public class AvroEventDeserializer extends AbstractAvroEventDeserializer {

  private static final Logger logger = LoggerFactory.getLogger(AvroEventDeserializer.class);

  private AvroEventDeserializer(Context context, ResettableInputStream ris) {
    this.ris = ris;

    schemaType = AvroSchemaType.valueOf(
        context.getString(CONFIG_SCHEMA_TYPE_KEY,
            AvroSchemaType.HASH.toString()).toUpperCase(Locale.ENGLISH));
    if (schemaType == AvroSchemaType.LITERAL) {
      logger.warn(CONFIG_SCHEMA_TYPE_KEY + " set to " +
          AvroSchemaType.LITERAL.toString() + ", so storing full Avro " +
          "schema in the header of each event, which may be inefficient. " +
          "Consider using the hash of the schema " +
          "instead of the literal schema.");
    }
  }

  public static class Builder implements EventDeserializer.Builder {

    @Override
    public EventDeserializer build(Context context, ResettableInputStream in) {
      if (!(in instanceof RemoteMarkable)) {
        throw new IllegalArgumentException("Cannot use this deserializer " +
                "without a RemoteMarkable input stream");
      }
      AvroEventDeserializer deserializer
              = new AvroEventDeserializer(context, in);
      try {
        deserializer.initialize(deserializer.ris);
      } catch (Exception e) {
        throw new FlumeException("Cannot instantiate deserializer", e);
      }
      return deserializer;
    }
  }

}