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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.google.common.base.Preconditions;

/**
 * This takes a regex and a group index and attaches an attribute with the
 * extracted value given attribute. This searches for the first instance of the
 * regex -- the entire body does not have to match. If the index is out of
 * bounds, or there is no match we return an empty string, "". 0 is the full
 * expression, and any number n>0 refers to the group enclosed by the nth left
 * paren.
 * 
 * For example: the group index and regex combo of: 3, (\d+):(\d+):(\d+)
 * 
 * "123:456:789" -> "798"
 * 
 * abc:def:xyz -> "" (no match)
 * 
 * 11:22 -> "" (out of range)
 * 
 * 55:66:33:22 -> "33" (ignores extras)
 * 
 * NOTE: the NFA-based regex algorithm used by java.util.regex.* (and in this
 * class) is slow and does not scale. It is fully featured but has an
 * exponential worst case running time.
 */
public class RegexExtractor extends EventSinkDecorator<EventSink> {
  final String attr;
  final Pattern pat;
  final int grp;

  /**
   * This will not thrown an exception
   */
  public RegexExtractor(EventSink snk, Pattern pat, int grp, String attr) {
    super(snk);
    this.attr = attr;
    this.pat = pat;
    this.grp = grp;
  }

  /**
   * Convenience constructor that may throw a PatternSyntaxException (runtime
   * exn).
   */
  public RegexExtractor(EventSink snk, String regex, int grp, String attr) {
    this(snk, Pattern.compile(regex), grp, attr);
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    String s = new String(e.getBody());
    Matcher m = pat.matcher(s);
    String val = ""; // default
    try {
      val = m.find() ? m.group(grp) : "";
    } catch (IndexOutOfBoundsException ioobe) {
      val = "";
    }
    Attributes.setString(e, attr, val);
    super.append(e);
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 3,
            "usage: regex(regex, idx, dstAttr)");

        String regex = argv[0];
        Integer idx = Integer.parseInt(argv[1]);
        Pattern pat = Pattern.compile(regex);
        String attr = argv[2];

        EventSinkDecorator<EventSink> snk = new RegexExtractor(null, pat, idx,
            attr);
        return snk;

      }
    };
  }
}
