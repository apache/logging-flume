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
package com.cloudera.flume.core.extractors;

import java.io.IOException;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.google.common.base.Preconditions;

/**
 * Give a regular expression to split on, split the body in chunks and assign
 * the attribute with its value. Currently this uses the String.split function.
 * 
 * TODO (jon) do not convert to string -- instead work directly with byte
 * sequences.
 */
public class SplitExtractor extends EventSinkDecorator<EventSink> {
  final String attr;
  final int idx;
  final String splitRegex;

  public SplitExtractor(EventSink s, String splitRegex, int tok, String attr) {
    super(s);
    this.splitRegex = splitRegex;
    this.idx = tok;
    this.attr = attr;
  }

  @Override
  public void append(Event e) throws IOException {
    String vals[] = new String(e.getBody()).split(splitRegex);
    String val = (idx < vals.length) ? vals[idx] : "";
    Attributes.setString(e, attr, val);
    super.append(e);
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 3,
            "usage: split(splitRegex, idx, dstAttr)");

        String regex = argv[0];
        Integer idx = Integer.parseInt(argv[1]);
        String attr = argv[2];

        EventSinkDecorator<EventSink> snk = new SplitExtractor(null, regex,
            idx, attr);
        return snk;

      }
    };
  }

}
