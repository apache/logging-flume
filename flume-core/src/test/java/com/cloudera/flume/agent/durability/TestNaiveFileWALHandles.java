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
package com.cloudera.flume.agent.durability;

import java.io.File;
import java.io.IOException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.handlers.hdfs.SeqfileEventSink;
import com.cloudera.util.FlumeTestHarness;

/**
 * Test for file handle exhaustion problems with WAL and DFO
 */
public class TestNaiveFileWALHandles {
  public static final Logger LOG = LoggerFactory
      .getLogger(TestNaiveFileWALHandles.class);

  /*
   * This tests to see if the seqfile event sink releases file handles.
   * 
   * Default handle limit per process is around 1000 so this should be ample to
   * cause problems.
   */
  @Test
  public void testSeqfileEventSinkHandleExhaust() throws IOException, InterruptedException {
    FlumeTestHarness.setupLocalWriteDir();
    File tmp = FlumeTestHarness.tmpdir;

    for (int i = 0; i < 3000; i++) {
      File path = new File(tmp, "" + i);
      EventSink snk = new SeqfileEventSink(path);
      snk.open();
      Event e = new EventImpl(("foo " + i).getBytes());
      snk.append(e);
      snk.close();
    }

  }

}
