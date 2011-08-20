/**
 * Copyright 2011 The Apache Software Foundation
 *
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
package com.cloudera.flume.agent;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.agent.durability.NaiveFileWALManager;
import com.cloudera.flume.agent.durability.WALManager;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.util.FileUtil;

public class TestFlumeNodeWALNotifier {

  String tag;
  Date date;
  Tagger mockTagger;
  AckListener mockAl;
  File tmpdir;
  NaiveFileWALManager walman;
  Map<String, WALManager> map;
  EventSink snk;
  EventSource src;

  @Before
  public void setup() throws IOException, InterruptedException {
    tag = "foofootag";
    date = new Date();
    mockTagger = mock(Tagger.class);
    when(mockTagger.getTag()).thenReturn("unused-" + tag);
    when(mockTagger.newTag()).thenReturn(tag);
    when(mockTagger.getDate()).thenReturn(date);

    mockAl = mock(AckListener.class);

    tmpdir = FileUtil.mktempdir();
    walman = new NaiveFileWALManager(tmpdir);
    walman.open();

    map = new HashMap<String, WALManager>();
    map.put("wal", walman);

    snk = walman.newAckWritingSink(mockTagger, mockAl);
  }

  @After
  public void teardown() throws IOException {
    FileUtil.rmr(tmpdir);
  }

  public void triggerRetry() throws IOException {
    FlumeNodeWALNotifier notif = new FlumeNodeWALNotifier(map);
    notif.retry(tag);
  }

  void toWritingState() throws IOException, InterruptedException {
    EventImpl evt = new EventImpl("foofoodata".getBytes());
    snk.open();
    snk.append(evt);

    assertEquals(1, walman.getWritingTags().size());
    assertEquals(0, walman.getLoggedTags().size());
    assertEquals(0, walman.getSendingTags().size());
    assertEquals(0, walman.getSentTags().size());
  }

  void toLoggedState() throws IOException, InterruptedException {
    // transition to logged state.
    snk.close();

    assertEquals(0, walman.getWritingTags().size());
    assertEquals(1, walman.getLoggedTags().size());
    assertEquals(0, walman.getSendingTags().size());
    assertEquals(0, walman.getSentTags().size());
  }

  void toSendingState() throws IOException, InterruptedException {
    // transition to sending state.
    src = walman.getUnackedSource();
    src.open();
    while (src.next() != null) {
      ;
    }
    assertEquals(0, walman.getWritingTags().size());
    assertEquals(0, walman.getLoggedTags().size());
    assertEquals(1, walman.getSendingTags().size());
    assertEquals(0, walman.getSentTags().size());
  }

  void toSentState() throws IOException, InterruptedException {
    // transition to sent state.
    src.close();
    assertEquals(0, walman.getWritingTags().size());
    assertEquals(0, walman.getLoggedTags().size());
    assertEquals(0, walman.getSendingTags().size());
    assertEquals(1, walman.getSentTags().size());
  }

  void toAckedState() throws IOException {
    // transition to acked state.
    walman.toAcked(tag);
    assertEquals(0, walman.getWritingTags().size());
    assertEquals(0, walman.getLoggedTags().size());
    assertEquals(0, walman.getSendingTags().size());
    assertEquals(0, walman.getSentTags().size());
  }

  /**
   * If we attempt to retry something in writing state (whose tag doesn't have
   * complete checksum, and thus should never be retried) it should fail with
   * IllegalStateException.
   */
  @Test(expected = IllegalStateException.class)
  public void testRetryWriting() throws IOException, InterruptedException {
    toWritingState();

    assertEquals(1, walman.getWritingTags().size());
    assertEquals(0, walman.getLoggedTags().size());
    assertEquals(0, walman.getSendingTags().size());
    assertEquals(0, walman.getSentTags().size());

    triggerRetry();

  }

  @Test
  public void testRetryLogged() throws IOException, InterruptedException {
    toWritingState();
    toLoggedState();

    triggerRetry();

    assertEquals(0, walman.getWritingTags().size());
    assertEquals(1, walman.getLoggedTags().size());
    assertEquals(0, walman.getSendingTags().size());
    assertEquals(0, walman.getSentTags().size());
  }

  @Test
  public void testRetrySending() throws IOException, InterruptedException {
    toWritingState();
    toLoggedState();
    toSendingState();

    triggerRetry();

    assertEquals(0, walman.getWritingTags().size());
    assertEquals(0, walman.getLoggedTags().size());
    assertEquals(1, walman.getSendingTags().size());
    assertEquals(0, walman.getSentTags().size());
  }

  @Test
  public void testRetrySent() throws IOException, InterruptedException {
    toWritingState();
    toLoggedState();
    toSendingState();
    toSentState();

    triggerRetry();

    assertEquals(0, walman.getWritingTags().size());
    assertEquals(1, walman.getLoggedTags().size());
    assertEquals(0, walman.getSendingTags().size());
    assertEquals(0, walman.getSentTags().size());
  }

  @Test
  public void testRetryAcked() throws IOException, InterruptedException {
    toWritingState();
    toLoggedState();
    toSendingState();
    toSentState();
    toAckedState();

    triggerRetry();

    assertEquals(0, walman.getWritingTags().size());
    assertEquals(0, walman.getLoggedTags().size());
    assertEquals(0, walman.getSendingTags().size());
    assertEquals(0, walman.getSentTags().size());
  }

}
