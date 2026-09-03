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

import java.io.IOException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.junit.Before;
import org.junit.Test;

public class TestRegexLineDeserializer {

  private String fileInfo;

  @Before
  public void setup() {
    StringBuilder sb = new StringBuilder();
    sb.append("2016-10-19 10:19:10,188 INFO  - Configuration provider starting");
    sb.append("\n");
    sb.append("2016-10-19 10:19:10,199 DEBUG - Starting validation of for agent: agent");
    sb.append("\n");
    sb.append("SOURCES: {s1={ parameters:{decodeErrorPolicy=REPLACE} }}");
    sb.append("\n");
    sb.append("CHANNELS: {c1={ parameters:{checkpointDir=./agent/checkpoint} }}");
    sb.append("\n");
    sb.append("SINKS: {k1={ parameters:{clusterName=es, indexType=test4} }}");
    sb.append("\n");
    sb.append("\n");
    sb.append("2016-10-19 10:19:10,204 DEBUG - Created channel c1");
    sb.append("\n");
    fileInfo = sb.toString();
  }
  
  @Test
  public void testMultiLine() throws IOException {
    // ^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}
    // ^((${srcStartWithString})(\\s|\\S)).*
    Context context = new Context();
    context.put("filePattern", "^((\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3})(\\s|\\S)).*");
    ResettableInputStream in = new TestRegexLineInputStream(fileInfo);
    EventDeserializer des = new RegexLineDeserializer(context, in);
    Event evt = des.readEvent();
    String line1 = new String(evt.getBody(), "UTF-8");
    System.out.println("line 1\n" + line1);
    des.mark();
    evt = des.readEvent();
    String line2 = new String(evt.getBody(), "UTF-8");
    System.out.println("line 2\n" + line2);
    des.mark();
    evt = des.readEvent();
    String line3 = new String(evt.getBody(), "UTF-8");
    System.out.println("line 3\n" + line3);
    des.reset();
    des.mark();
    des.close();
  }
}
