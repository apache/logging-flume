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

import java.io.IOException;
import java.io.OutputStream;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class simply writes the body of the event to the output stream
 * and appends a newline after each event.
 */
public class HeaderAndBodyTextEventSerializer implements EventSerializer {

  private static final Logger logger =
      LoggerFactory.getLogger(HeaderAndBodyTextEventSerializer.class);

  // for legacy reasons, by default, append a newline to each event written out
  private final String APPEND_NEWLINE = "appendNewline";
  private final boolean APPEND_NEWLINE_DFLT = true;

  private final OutputStream out;
  private final boolean appendNewline;

  private HeaderAndBodyTextEventSerializer(OutputStream out, Context ctx) {
    this.appendNewline = ctx.getBoolean(APPEND_NEWLINE, APPEND_NEWLINE_DFLT);
    this.out = out;
  }

  @Override
  public boolean supportsReopen() {
    return true;
  }

  @Override
  public void afterCreate() {
    // noop
  }

  @Override
  public void afterReopen() {
    // noop
  }

  @Override
  public void beforeClose() {
    // noop
  }

  @Override
  public void write(Event e) throws IOException {
    out.write((e.getHeaders() + " ").getBytes());
    out.write(e.getBody());
    if (appendNewline) {
      out.write('\n');
    }
  }

  @Override
  public void flush() throws IOException {
    // noop
  }

  public static class Builder implements EventSerializer.Builder {

    @Override
    public EventSerializer build(Context context, OutputStream out) {
      HeaderAndBodyTextEventSerializer s = new HeaderAndBodyTextEventSerializer(out, context);
      return s;
    }

  }

}
