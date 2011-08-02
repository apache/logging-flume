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
package com.cloudera.flume.handlers.console;

import java.io.IOException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.StandardSourceSinkHarnesses;

public class TestJLineStdinSource {
  public static final Logger LOG = LoggerFactory
      .getLogger(TestJLineStdinSource.class);

  @Test
  public void testCloseClose() throws IOException, InterruptedException {
    EventSource src = new JLineStdinSource();
    StandardSourceSinkHarnesses.testCloseClose(LOG, src);
  }

  @Test
  public void testOpenOpen() throws IOException, InterruptedException {
    EventSource src = new JLineStdinSource();
    StandardSourceSinkHarnesses.testOpenOpen(LOG, src);
  }

  @Test
  public void testOpenClose() throws InterruptedException, IOException {
    EventSource src = new JLineStdinSource();
    StandardSourceSinkHarnesses.testSourceOpenClose(LOG, src);
  }

  @Test
  public void testConcurrentClose() throws InterruptedException, IOException {
    EventSource src = new JLineStdinSource();
    StandardSourceSinkHarnesses.testSourceConcurrentClose(LOG, src);
  }
}
