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

package com.cloudera.flume.handlers.debug;

import java.io.IOException;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;

/**
 * This converts all of the body data to bytes that are ascii printable with no
 * newlines. Good for testing and generating arbitrary workloads
 */
public class NoNlASCIISynthSource extends SynthSource {

  public NoNlASCIISynthSource(long count, int size, long seed) {
    super(count, size, seed);
  }

  public NoNlASCIISynthSource(long count, int size) {
    super(count, size);
  }

  /**
   * Converts all bytes into the ascii pritable range (32 >= 126),
   */
  static byte toAscii(byte b) {
    b &= 0x7f;
    if (b >= 0 && b < 32)
      return ' ';
    if (b == 127)
      return ' ';
    if (b == '\n')
      return ' ';
    return b;
  }

  public Event next() throws IOException {
    Event e = super.next();
    if (e == null)
      return null;

    // NOTE: this is a reference to the body. and will be modified
    byte[] body = e.getBody();
    for (int i = 0; i < body.length; i++) {
      body[i] = toAscii(body[i]);
    }

    return e;
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(Context ctx, String... argv) {
        int size = 150;
        long count = 0;
        if (argv.length > 2) {
          throw new IllegalArgumentException(
              "usage: asciisynth([count=0 [,randsize=150]]) // count=0 infinite");
        }
        if (argv.length >= 1) {
          count = Long.parseLong(argv[0]);
        }
        if (argv.length >= 2)
          size = Integer.parseInt(argv[1]);
        return new NoNlASCIISynthSource(count, size);
      }

    };
  }
}
