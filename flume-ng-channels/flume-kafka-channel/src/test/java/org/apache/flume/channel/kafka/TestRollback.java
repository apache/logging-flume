/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel.kafka;

import org.apache.flume.Event;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;

public class TestRollback extends TestKafkaChannelBase {

  @Test
  public void testSuccess() throws Exception {
    doTestSuccessRollback(false, false);
  }

  @Test
  public void testSuccessInterleave() throws Exception {
    doTestSuccessRollback(false, true);
  }

  @Test
  public void testRollbacks() throws Exception {
    doTestSuccessRollback(true, false);
  }

  @Test
  public void testRollbacksInterleave() throws Exception {
    doTestSuccessRollback(true, true);
  }

  private void doTestSuccessRollback(final boolean rollback,
                                     final boolean interleave) throws Exception {
    final KafkaChannel channel = startChannel(true);
    writeAndVerify(rollback, channel, interleave);
    channel.stop();
  }

  private void writeAndVerify(final boolean testRollbacks,
                              final KafkaChannel channel, final boolean interleave)
      throws Exception {

    final List<List<Event>> events = createBaseList();

    ExecutorCompletionService<Void> submitterSvc =
        new ExecutorCompletionService<Void>(Executors.newCachedThreadPool());

    putEvents(channel, events, submitterSvc);

    if (interleave) {
      wait(submitterSvc, 5);
    }

    ExecutorCompletionService<Void> submitterSvc2 =
        new ExecutorCompletionService<Void>(Executors.newCachedThreadPool());

    final List<Event> eventsPulled = pullEvents(channel, submitterSvc2, 50, testRollbacks, true);

    if (!interleave) {
      wait(submitterSvc, 5);
    }
    wait(submitterSvc2, 5);

    verify(eventsPulled);
  }
}
