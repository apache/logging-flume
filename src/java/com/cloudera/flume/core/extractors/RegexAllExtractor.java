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
import java.util.ArrayList;
import java.util.List;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.google.common.base.Preconditions;

/**
 * This takes a regex and any number of attribute names to assign to each
 * sub pattern in pattern order
 *
 * Example 1:
 *   regexAll("(\d+):(\d+):(\d+)", s1, s2, s3)
 *
 *   "123:456:789" -> {s1:123, s2:456, s3:789}
 *
 * Example 2:
 *   regexAll("(\d+):(\d+):(\d+)", s1, s2)
 *
 *   "123:456:789" -> {s1:123, s2:456}
 *
 * Example 3:
 *   regexAll("(\d+):(\d+):(\d+)", s1, "", s2)
 *
 *   "123:456:789" -> {s1:123, s2:789}
 */
public class RegexAllExtractor extends EventSinkDecorator<EventSink> {
  
  final Pattern pat;
  final List<String> names;

  /**
   * This will not thrown an exception
   */
  public RegexAllExtractor(EventSink snk, Pattern pat, List<String> names) {
    super(snk);
    this.pat = pat;
    this.names = names;
  }

  /**
   * Convenience constructor that may throw a PatternSyntaxException (runtime
   * exn).
   */
  public RegexAllExtractor(EventSink snk, String regex, List<String> names) {
    this(snk, Pattern.compile(regex), names);
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    String s = new String(e.getBody());
    Matcher m = pat.matcher(s);
    String val = "";
    Integer grpCnt = m.groupCount();

    if(m.find()){
      for(int grp = 1; grp <= grpCnt; grp++){
        val = "";
        try {
          val = m.group(grp);
        } catch (IndexOutOfBoundsException ioobe) {
          val = "";
        }

        //Try/Catch so that we don't require there be the same number of names as patterns.
        try {
          //Ignore blank names. These are most likely sub patterns we don't care about keeping.
          if(names.get(grp-1) != ""){
            Attributes.setString(e, names.get(grp-1), val);
          }
        } catch (IndexOutOfBoundsException ioobe) {
          break;
        }
      }
    }
    super.append(e);
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length >= 2, "usage: regexAll(regex, attr[, attr])");

        String regex = argv[0];
        Pattern pat = Pattern.compile(regex);
        ArrayList<String> names = new ArrayList<String>();
        for(int i=1; i<argv.length; ++i){
          names.add(argv[i]);
        }

        EventSinkDecorator<EventSink> snk = new RegexAllExtractor(null, pat, names);
        return snk;

      }
    };
  }
}
