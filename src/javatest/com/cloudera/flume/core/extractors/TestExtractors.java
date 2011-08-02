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

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Test;

import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.handlers.debug.MemorySinkSource;

/**
 * Tests the behavior of the split and regex extractors.
 */
public class TestExtractors {

  @Test
  public void testRegexExtractor() throws IOException {
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    RegexExtractor re1 = new RegexExtractor(mem, "(\\d:\\d)", 1, "colon");
    RegexExtractor re2 = new RegexExtractor(re1, "(.+)oo(.+)", 1, "oo");
    RegexExtractor re3 = new RegexExtractor(re2, "(.+)oo(.+)", 0, "full");
    RegexExtractor re4 = new RegexExtractor(re3, "(blah)blabh", 3, "empty");
    RegexExtractor re = new RegexExtractor(re4, "(.+)oo(.+)", 3, "outofrange");

    re.open();
    re.append(new EventImpl("1:2:3.4foobar5".getBytes()));
    re.close();

    mem.close();
    mem.open();
    Event e1 = mem.next();
    assertEquals("1:2", Attributes.readString(e1, "colon"));
    assertEquals("1:2:3.4f", Attributes.readString(e1, "oo"));
    assertEquals("1:2:3.4foobar5", Attributes.readString(e1, "full"));
    assertEquals("", Attributes.readString(e1, "empty"));
    assertEquals("", Attributes.readString(e1, "outofrange"));
  }

  @Test
  public void testSplitExtractor() throws IOException {
    MemorySinkSource mem = new MemorySinkSource();
    SplitExtractor re1 = new SplitExtractor(mem, "\\.", 1, "dot");
    SplitExtractor re2 = new SplitExtractor(re1, ":", 1, "colon");
    SplitExtractor re3 = new SplitExtractor(re2, "foobar", 1, "foobar");
    SplitExtractor re4 = new SplitExtractor(re3, "#", 1, "empty");
    SplitExtractor re5 = new SplitExtractor(re4, "foobar", 2, "outofrange");
    SplitExtractor re = new SplitExtractor(re5, "\\.|:|foobar", 3, "disj");

    re.open();
    re.append(new EventImpl("1:2:3.4foobar5".getBytes()));
    re.close();

    mem.close();
    mem.open();
    Event e1 = mem.next();
    assertEquals("4foobar5", Attributes.readString(e1, "dot"));
    assertEquals("2", Attributes.readString(e1, "colon"));
    assertEquals("5", Attributes.readString(e1, "foobar"));
    assertEquals("4", Attributes.readString(e1, "disj"));
    assertEquals("", Attributes.readString(e1, "empty"));
    assertEquals("", Attributes.readString(e1, "outofrange"));

  }
}
