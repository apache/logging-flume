/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.handlers.text.output;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.handlers.text.FormatFactory.OutputFormatBuilder;
import com.google.common.base.Preconditions;

/**
 * This uses Jackson to output Flume events as json.
 */
public class JsonOutputFormat extends AbstractOutputFormat {

  private static final String NAME = "json";
  private final JsonFactory jsonFactory = new JsonFactory();

  public JsonOutputFormat() {
  }

  @Override
  public void format(OutputStream o, Event e) throws IOException {
    JsonGenerator g = jsonFactory.createJsonGenerator(o, JsonEncoding.UTF8);
    g.writeStartObject();
    g.writeStringField("body", new String(e.getBody()));
    g.writeNumberField("timestamp", e.getTimestamp());
    g.writeStringField("pri", e.getPriority().toString());
    g.writeNumberField("nanos", e.getNanos());
    g.writeStringField("host", e.getHost());
    g.writeObjectFieldStart("fields");
    for (Entry<String, byte[]> a : e.getAttrs().entrySet()) {
      g.writeStringField(a.getKey(), new String(a.getValue()));
    }
    g.writeEndObject();
    g.writeEndObject();
    g.writeRaw('\n');
    g.flush();
  }

  public static OutputFormatBuilder builder() {
    return new OutputFormatBuilder() {

      @Override
      public OutputFormat build(String... args) {
        Preconditions.checkArgument(args.length == 0, "usage: json");

        OutputFormat format = new JsonOutputFormat();
        format.setBuilder(this);

        return format;
      }

      @Override
      public String getName() {
        return NAME;
      }

    };
  }
}
