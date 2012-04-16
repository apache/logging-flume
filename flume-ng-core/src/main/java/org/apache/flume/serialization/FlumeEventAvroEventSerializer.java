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
package org.apache.flume.serialization;

import org.apache.flume.serialization.AbstractAvroEventSerializer;
import java.io.OutputStream;
import org.apache.avro.Schema;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.EventSerializer;

public class FlumeEventAvroEventSerializer extends AbstractAvroEventSerializer<Event> {

  private static final Schema SCHEMA = new Schema.Parser().parse(
      "{ \"type\":\"record\", \"name\": \"Event\", \"fields\": [" +
      " {\"name\": \"headers\", \"type\": { \"type\": \"map\", \"values\": \"string\" } }, " +
      " {\"name\": \"body\", \"type\": \"bytes\" } ] }");

  private final OutputStream out;

  private FlumeEventAvroEventSerializer(OutputStream out) {
    this.out = out;
  }

  @Override
  protected Schema getSchema() {
    return SCHEMA;
  }

  @Override
  protected OutputStream getOutputStream() {
    return out;
  }

  /**
   * A no-op for this simple, special-case implementation
   * @param event
   * @return
   */
  @Override
  protected Event convert(Event event) {
    return event;
  }

  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(Context context, OutputStream out) {
      FlumeEventAvroEventSerializer writer = new FlumeEventAvroEventSerializer(out);
      writer.configure(context);
      return writer;
    }

  }

}
