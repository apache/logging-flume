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
package com.cloudera.flume.handlers.hdfs;

import java.io.File;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;

/**
 * This tests a read of an existing logs.hdfs file.
 * 
 * TODO (jon) Update this test because of change in event format.
 */
public class TestReadLogEvents {

  @Test
  public void testReadLogEvents() throws IOException, InterruptedException {
    File tmp = File.createTempFile("test", "tmp");
    tmp.deleteOnExit();
    SeqfileEventSink sink = new SeqfileEventSink(tmp);
    sink.open();
    for (int i = 0; i < 100; i++) {
      Event e = new EventImpl(("test " + i).getBytes());
      sink.append(e);
    }
    sink.close();

    SeqfileEventSource src = SeqfileEventSource
        .openLocal(tmp.getAbsolutePath());

    int i = 0;
    for (Event e = src.next(); e != null; e = src.next()) {
      System.out.println("" + i + " " + e);
      i++;
    }
    Assert.assertEquals(i, 100);
  }
}
