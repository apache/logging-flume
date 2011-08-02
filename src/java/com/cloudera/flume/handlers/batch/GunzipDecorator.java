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
package com.cloudera.flume.handlers.batch;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.handlers.hdfs.WriteableEvent;
import com.google.common.base.Preconditions;

/**
 * This gunzip's any event that is gzip'ed, otherwise events just pass through.
 */
public class GunzipDecorator<S extends EventSink> extends EventSinkDecorator<S> {
  public GunzipDecorator(S s) {
    super(s);
  }

  public final static String GZDOC = "compressGzip";

  public static boolean isGzEven(Event e) {
    return e.get(GZDOC) != null;
  }

  @Override
  public void append(Event e) throws IOException {

    byte[] bs = e.get(GZDOC);
    if (bs == null) {
      super.append(e);
      return;
    }

    ByteArrayInputStream bais = new ByteArrayInputStream(bs);
    GZIPInputStream gzis = new GZIPInputStream(bais);
    DataInputStream dis = new DataInputStream(gzis);

    WriteableEvent out = new WriteableEvent();
    out.readFields(dis);
    dis.close();
    super.append(out);
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 0, "usage: gunzip");
        return new GunzipDecorator<EventSink>(null);
      }
    };
  }
}
