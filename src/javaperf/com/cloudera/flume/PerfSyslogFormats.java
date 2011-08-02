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
package com.cloudera.flume;

import java.io.IOException;

import junit.framework.TestCase;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.debug.NullSink;
import com.cloudera.flume.handlers.debug.TextFileSource;
import com.cloudera.flume.handlers.text.EventExtractException;
import com.cloudera.flume.handlers.text.SyslogEntryFormat;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.util.Benchmark;

/**
 * Performance testing for the throughput of syslog and apache log format
 * parsing.
 */
public class PerfSyslogFormats extends TestCase implements ExamplePerfData {

  public void testSyslogFormat() throws IOException, EventExtractException {
    Benchmark b = new Benchmark("Syslog format + nullsink");
    b.mark("begin");
    TextFileSource txt = new TextFileSource(SYSLOG_LOG); // 23244 entires
    txt.open();
    MemorySinkSource mem = new MemorySinkSource();
    mem.open();
    EventUtil.dumpAll(txt, mem);
    txt.close();

    b.mark("disk_loaded");
    Event e = null;
    NullSink sink = new NullSink();
    SyslogEntryFormat syslog = new SyslogEntryFormat();
    while ((e = mem.next()) != null) {
      Event e2 = syslog.extract(new String(e.getBody()), 2009);
      sink.append(e2);
    }
    sink.close();
    b.mark("warmup done");

    e = null;
    mem.open();
    while ((e = mem.next()) != null) {
      Event e2 = syslog.extract(new String(e.getBody()), 2009);
      sink.append(e2);
    }
    sink.close();
    b.mark("sample dump done");

    e = null;
    mem.open();
    CounterSink sink2 = new CounterSink("counter");

    while ((e = mem.next()) != null) {
      Event e2 = syslog.extract(new String(e.getBody()), 2009);
      sink2.append(e2);
    }
    sink2.close();
    b.mark("count done", sink2.getCount());

    b.done();
  }

}
