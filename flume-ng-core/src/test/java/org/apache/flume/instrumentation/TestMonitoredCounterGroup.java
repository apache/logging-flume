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
package org.apache.flume.instrumentation;

import java.lang.management.ManagementFactory;
import java.util.Random;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class TestMonitoredCounterGroup {

  private static final int MAX_BOUNDS = 1000;
  private static final String ROOT_OBJ_NAME_PREFIX = "org.apache.flume.";
  private static final String SOURCE_OBJ_NAME_PREFIX = ROOT_OBJ_NAME_PREFIX
      + "source:type=";
  private static final String CHANNEL_OBJ_NAME_PREFIX = ROOT_OBJ_NAME_PREFIX
      + "channel:type=";
  private static final String SINK_OBJ_NAME_PREFIX = ROOT_OBJ_NAME_PREFIX
      + "sink:type=";

  private static final String ATTR_START_TIME = "StartTime";
  private static final String ATTR_STOP_TIME = "StopTime";

  private static final String SRC_ATTR_EVENT_RECEVIED_COUNT =
      "EventReceivedCount";
  private static final String SRC_ATTR_EVENT_ACCEPTED_COUNT =
      "EventAcceptedCount";
  private static final String SRC_ATTR_APPEND_RECEVIED_COUNT =
      "AppendReceivedCount";
  private static final String SRC_ATTR_APPEND_ACCEPTED_COUNT =
      "AppendAcceptedCount";
  private static final String SRC_ATTR_APPEND_BATCH_RECEVIED_COUNT =
      "AppendBatchReceivedCount";
  private static final String SRC_ATTR_APPEND_BATCH_ACCEPTED_COUNT =
      "AppendBatchAcceptedCount";


  private static final String CH_ATTR_CHANNEL_SIZE = "ChannelSize";
  private static final String CH_ATTR_EVENT_PUT_ATTEMPT =
      "EventPutAttemptCount";
  private static final String CH_ATTR_EVENT_TAKE_ATTEMPT =
      "EventTakeAttemptCount";
  private static final String CH_ATTR_EVENT_PUT_SUCCESS =
      "EventPutSuccessCount";
  private static final String CH_ATTR_EVENT_TAKE_SUCCESS =
      "EventTakeSuccessCount";

  private static final String SK_ATTR_CONN_CREATED =
      "ConnectionCreatedCount";
  private static final String SK_ATTR_CONN_CLOSED =
      "ConnectionClosedCount";
  private static final String SK_ATTR_CONN_FAILED =
      "ConnectionFailedCount";
  private static final String SK_ATTR_BATCH_EMPTY =
      "BatchEmptyCount";
  private static final String SK_ATTR_BATCH_UNDERFLOW =
      "BatchUnderflowCount";
  private static final String SK_ATTR_BATCH_COMPLETE =
      "BatchCompleteCount";
  private static final String SK_ATTR_EVENT_DRAIN_ATTEMPT =
      "EventDrainAttemptCount";
  private static final String SK_ATTR_EVENT_DRAIN_SUCCESS =
      "EventDrainSuccessCount";

  private MBeanServer mbServer;
  private Random random;

  @Before
  public void setUp() {
    mbServer = ManagementFactory.getPlatformMBeanServer();
    random = new Random(System.nanoTime());
  }

  @Test
  public void testSinkCounter() throws Exception {
    String name = getRandomName();

    SinkCounter skc = new SinkCounter(name);
    skc.register();
    ObjectName on = new ObjectName(SINK_OBJ_NAME_PREFIX + name);
    assertSkCounterState(on, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

    skc.start();
    long start1 = getStartTime(on);

    Assert.assertTrue("StartTime", start1 != 0L);
    Assert.assertTrue("StopTime", getStopTime(on) == 0L);

    int connCreated = random.nextInt(MAX_BOUNDS);
    int connClosed = random.nextInt(MAX_BOUNDS);
    int connFailed = random.nextInt(MAX_BOUNDS);
    int batchEmpty = random.nextInt(MAX_BOUNDS);
    int batchUnderflow = random.nextInt(MAX_BOUNDS);
    int batchComplete = random.nextInt(MAX_BOUNDS);
    int eventDrainAttempt = random.nextInt(MAX_BOUNDS);
    int eventDrainSuccess = random.nextInt(MAX_BOUNDS);

    for (int i = 0; i<connCreated; i++) {
      skc.incrementConnectionCreatedCount();
    }
    for (int i = 0; i<connClosed; i++) {
      skc.incrementConnectionClosedCount();
    }
    for (int i = 0; i<connFailed; i++) {
      skc.incrementConnectionFailedCount();
    }
    for (int i = 0; i<batchEmpty; i++) {
      skc.incrementBatchEmptyCount();
    }
    for (int i = 0; i<batchUnderflow; i++) {
      skc.incrementBatchUnderflowCount();
    }
    for (int i = 0; i<batchComplete; i++) {
      skc.incrementBatchCompleteCount();
    }
    for (int i = 0; i<eventDrainAttempt; i++) {
      skc.incrementEventDrainAttemptCount();
    }
    for (int i = 0; i<eventDrainSuccess; i++) {
      skc.incrementEventDrainSuccessCount();
    }

    assertSkCounterState(on, connCreated, connClosed, connFailed, batchEmpty,
        batchUnderflow, batchComplete, eventDrainAttempt, eventDrainSuccess);

    skc.stop();

    Assert.assertTrue("StartTime", getStartTime(on) != 0L);
    Assert.assertTrue("StopTime", getStopTime(on) != 0L);

    assertSkCounterState(on, connCreated, connClosed, connFailed, batchEmpty,
        batchUnderflow, batchComplete, eventDrainAttempt, eventDrainSuccess);

    // give start time a chance to increment
    Thread.sleep(5L);

    skc.start();
    Assert.assertTrue("StartTime", getStartTime(on) != 0L);
    Assert.assertTrue("StartTime", getStartTime(on) > start1);
    Assert.assertTrue("StopTime", getStopTime(on) == 0L);

    assertSkCounterState(on, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

    int eventDrainAttempt2 = random.nextInt(MAX_BOUNDS);
    int eventDrainSuccess2 = random.nextInt(MAX_BOUNDS);

    skc.addToEventDrainAttemptCount(eventDrainAttempt2);
    skc.addToEventDrainSuccessCount(eventDrainSuccess2);

    assertSkCounterState(on, 0L, 0L, 0L, 0L, 0L, 0L,
        eventDrainAttempt2, eventDrainSuccess2);
  }

  @Test
  public void testChannelCounter() throws Exception {
    String name = getRandomName();

    ChannelCounter chc = new ChannelCounter(name);
    chc.register();
    ObjectName on = new ObjectName(CHANNEL_OBJ_NAME_PREFIX + name);
    assertChCounterState(on, 0L, 0L, 0L, 0L, 0L);

    Assert.assertTrue("StartTime", getStartTime(on) == 0L);
    Assert.assertTrue("StopTime", getStopTime(on) == 0L);

    chc.start();

    long start1 = getStartTime(on);

    Assert.assertTrue("StartTime", start1 != 0L);
    Assert.assertTrue("StopTime", getStopTime(on) == 0L);

    int numChannelSize = random.nextInt(MAX_BOUNDS);
    int numEventPutAttempt = random.nextInt(MAX_BOUNDS);
    int numEventTakeAttempt = random.nextInt(MAX_BOUNDS);
    int numEventPutSuccess = random.nextInt(MAX_BOUNDS);
    int numEventTakeSuccess = random.nextInt(MAX_BOUNDS);

    chc.setChannelSize(numChannelSize);
    for (int i = 0; i<numEventPutAttempt; i++) {
      chc.incrementEventPutAttemptCount();
    }
    for (int i = 0; i<numEventTakeAttempt; i++) {
      chc.incrementEventTakeAttemptCount();
    }
    chc.addToEventPutSuccessCount(numEventPutSuccess);
    chc.addToEventTakeSuccessCount(numEventTakeSuccess);

    assertChCounterState(on, numChannelSize, numEventPutAttempt,
        numEventTakeAttempt, numEventPutSuccess, numEventTakeSuccess);

    chc.stop();

    Assert.assertTrue("StartTime", getStartTime(on) != 0L);
    Assert.assertTrue("StopTime", getStopTime(on) != 0L);

    assertChCounterState(on, numChannelSize, numEventPutAttempt,
        numEventTakeAttempt, numEventPutSuccess, numEventTakeSuccess);

    // give start time a chance to increment
    Thread.sleep(5L);

    chc.start();
    Assert.assertTrue("StartTime", getStartTime(on) != 0L);
    Assert.assertTrue("StartTime", getStartTime(on) > start1);
    Assert.assertTrue("StopTime", getStopTime(on) == 0L);

    assertChCounterState(on, 0L, 0L, 0L, 0L, 0L);
  }

  @Test
  public void testSourceCounter() throws Exception {
    String name = getRandomName();

    SourceCounter srcc = new SourceCounter(name);
    srcc.register();
    ObjectName on = new ObjectName(SOURCE_OBJ_NAME_PREFIX + name);

    assertSrcCounterState(on, 0L, 0L, 0L, 0L, 0L, 0L);

    Assert.assertTrue("StartTime", getStartTime(on) == 0L);
    Assert.assertTrue("StopTime", getStopTime(on) == 0L);

    srcc.start();

    long start1 = getStartTime(on);

    Assert.assertTrue("StartTime", start1 != 0L);
    Assert.assertTrue("StopTime", getStopTime(on) == 0L);

    int numEventReceived = random.nextInt(MAX_BOUNDS);
    int numEventAccepted = random.nextInt(MAX_BOUNDS);
    int numAppendReceived = random.nextInt(MAX_BOUNDS);
    int numAppendAccepted = random.nextInt(MAX_BOUNDS);
    int numAppendBatchReceived = random.nextInt(MAX_BOUNDS);
    int numAppendBatchAccepted = random.nextInt(MAX_BOUNDS);

    srcc.addToEventReceivedCount(numEventReceived);
    srcc.addToEventAcceptedCount(numEventAccepted);
    for (int i = 0; i<numAppendReceived; i++) {
      srcc.incrementAppendReceivedCount();
    }
    for (int i = 0; i<numAppendAccepted; i++) {
      srcc.incrementAppendAcceptedCount();
    }
    for (int i = 0; i<numAppendBatchReceived; i++) {
      srcc.incrementAppendBatchReceivedCount();
    }
    for (int i = 0; i<numAppendBatchAccepted; i++) {
      srcc.incrementAppendBatchAcceptedCount();
    }

    assertSrcCounterState(on, numEventReceived, numEventAccepted,
        numAppendReceived, numAppendAccepted, numAppendBatchReceived,
        numAppendBatchAccepted);

    srcc.stop();
    Assert.assertTrue("StartTime", getStartTime(on) != 0L);
    Assert.assertTrue("StopTime", getStopTime(on) != 0L);

    assertSrcCounterState(on, numEventReceived, numEventAccepted,
        numAppendReceived, numAppendAccepted, numAppendBatchReceived,
        numAppendBatchAccepted);

    // give start time a chance to increment
    Thread.sleep(5L);

    srcc.start();
    Assert.assertTrue("StartTime", getStartTime(on) != 0L);
    Assert.assertTrue("StartTime", getStartTime(on) > start1);
    Assert.assertTrue("StopTime", getStopTime(on) == 0L);

    assertSrcCounterState(on, 0L, 0L, 0L, 0L, 0L, 0L);

    int numEventReceived2 = random.nextInt(MAX_BOUNDS);
    int numEventAccepted2 = random.nextInt(MAX_BOUNDS);

    for (int i = 0; i<numEventReceived2; i++) {
      srcc.incrementEventReceivedCount();
    }

    for (int i = 0; i<numEventAccepted2; i++) {
      srcc.incrementEventAcceptedCount();
    }

    assertSrcCounterState(on, numEventReceived2, numEventAccepted2,
        0L, 0L, 0L, 0L);
  }

  @Test
  public void testRegisterTwice() throws Exception {
    String name = "re-register-" + getRandomName();

    SourceCounter c1 = new SourceCounter(name);
    c1.register();
    ObjectName on = new ObjectName(SOURCE_OBJ_NAME_PREFIX + name);

    Assert.assertEquals("StartTime", 0L, getStartTime(on));
    Assert.assertEquals("StopTime", 0L, getStopTime(on));
    c1.start();
    c1.stop();
    Assert.assertTrue("StartTime", getStartTime(on) > 0L);
    Assert.assertTrue("StopTime", getStopTime(on) > 0L);

    SourceCounter c2 = new SourceCounter(name);
    c2.register();

    Assert.assertEquals("StartTime", 0L, getStartTime(on));
    Assert.assertEquals("StopTime", 0L, getStopTime(on));
  }

  private void assertSrcCounterState(ObjectName on, long eventReceivedCount,
      long eventAcceptedCount, long appendReceivedCount,
      long appendAcceptedCount, long appendBatchReceivedCount,
      long appendBatchAcceptedCount) throws Exception {
    Assert.assertEquals("SrcEventReceived",
        getSrcEventReceivedCount(on),
        eventReceivedCount);
    Assert.assertEquals("SrcEventAccepted",
        getSrcEventAcceptedCount(on),
        eventAcceptedCount);
    Assert.assertEquals("SrcAppendReceived",
        getSrcAppendReceivedCount(on),
        appendReceivedCount);
    Assert.assertEquals("SrcAppendAccepted",
        getSrcAppendAcceptedCount(on),
        appendAcceptedCount);
    Assert.assertEquals("SrcAppendBatchReceived",
        getSrcAppendBatchReceivedCount(on),
        appendBatchReceivedCount);
    Assert.assertEquals("SrcAppendBatchAccepted",
        getSrcAppendBatchAcceptedCount(on),
        appendBatchAcceptedCount);
  }

  private void assertChCounterState(ObjectName on, long channelSize,
      long eventPutAttempt, long eventTakeAttempt, long eventPutSuccess,
      long eventTakeSuccess) throws Exception {
    Assert.assertEquals("ChChannelSize",
        getChChannelSize(on),
        channelSize);
    Assert.assertEquals("ChEventPutAttempt",
        getChEventPutAttempt(on),
        eventPutAttempt);
    Assert.assertEquals("ChEventTakeAttempt",
        getChEventTakeAttempt(on),
        eventTakeAttempt);
    Assert.assertEquals("ChEventPutSuccess",
        getChEventPutSuccess(on),
        eventPutSuccess);
    Assert.assertEquals("ChEventTakeSuccess",
        getChEventTakeSuccess(on),
        eventTakeSuccess);
  }

  private void assertSkCounterState(ObjectName on, long connCreated,
      long connClosed, long connFailed, long batchEmpty, long batchUnderflow,
      long batchComplete, long eventDrainAttempt, long eventDrainSuccess)
        throws Exception {
    Assert.assertEquals("SkConnCreated",
        getSkConnectionCreated(on),
        connCreated);
    Assert.assertEquals("SkConnClosed",
        getSkConnectionClosed(on),
        connClosed);
    Assert.assertEquals("SkConnFailed",
        getSkConnectionFailed(on),
        connFailed);
    Assert.assertEquals("SkBatchEmpty",
        getSkBatchEmpty(on),
        batchEmpty);
    Assert.assertEquals("SkBatchUnderflow",
        getSkBatchUnderflow(on),
        batchUnderflow);
    Assert.assertEquals("SkBatchComplete",
        getSkBatchComplete(on),
        batchComplete);
    Assert.assertEquals("SkEventDrainAttempt",
        getSkEventDrainAttempt(on),
        eventDrainAttempt);
    Assert.assertEquals("SkEventDrainSuccess",
        getSkEventDrainSuccess(on),
        eventDrainSuccess);
  }

  private long getStartTime(ObjectName on) throws Exception {
    return getLongAttribute(on, ATTR_START_TIME);
  }

  private long getStopTime(ObjectName on) throws Exception {
    return getLongAttribute(on, ATTR_STOP_TIME);
  }

  private long getSkConnectionCreated(ObjectName on) throws Exception {
    return getLongAttribute(on, SK_ATTR_CONN_CREATED);
  }

  private long getSkConnectionClosed(ObjectName on) throws Exception {
    return getLongAttribute(on, SK_ATTR_CONN_CLOSED);
  }

  private long getSkConnectionFailed(ObjectName on) throws Exception {
    return getLongAttribute(on, SK_ATTR_CONN_FAILED);
  }

  private long getSkBatchEmpty(ObjectName on) throws Exception {
    return getLongAttribute(on, SK_ATTR_BATCH_EMPTY);
  }

  private long getSkBatchUnderflow(ObjectName on) throws Exception {
    return getLongAttribute(on, SK_ATTR_BATCH_UNDERFLOW);
  }

  private long getSkBatchComplete(ObjectName on) throws Exception {
    return getLongAttribute(on, SK_ATTR_BATCH_COMPLETE);
  }

  private long getSkEventDrainAttempt(ObjectName on) throws Exception {
    return getLongAttribute(on, SK_ATTR_EVENT_DRAIN_ATTEMPT);
  }

  private long getSkEventDrainSuccess(ObjectName on) throws Exception {
    return getLongAttribute(on, SK_ATTR_EVENT_DRAIN_SUCCESS);
  }

  private long getChChannelSize(ObjectName on) throws Exception {
    return getLongAttribute(on, CH_ATTR_CHANNEL_SIZE);
  }

  private long getChEventPutAttempt(ObjectName on) throws Exception {
    return getLongAttribute(on, CH_ATTR_EVENT_PUT_ATTEMPT);
  }

  private long getChEventTakeAttempt(ObjectName on) throws Exception {
    return getLongAttribute(on, CH_ATTR_EVENT_TAKE_ATTEMPT);
  }

  private long getChEventPutSuccess(ObjectName on) throws Exception {
    return getLongAttribute(on, CH_ATTR_EVENT_PUT_SUCCESS);
  }

  private long getChEventTakeSuccess(ObjectName on) throws Exception {
    return getLongAttribute(on, CH_ATTR_EVENT_TAKE_SUCCESS);
  }

  private long getSrcAppendBatchAcceptedCount(ObjectName on) throws Exception {
    return getLongAttribute(on, SRC_ATTR_APPEND_BATCH_ACCEPTED_COUNT);
  }

  private long getSrcAppendBatchReceivedCount(ObjectName on) throws Exception {
    return getLongAttribute(on, SRC_ATTR_APPEND_BATCH_RECEVIED_COUNT);
  }

  private long getSrcAppendAcceptedCount(ObjectName on) throws Exception {
    return getLongAttribute(on, SRC_ATTR_APPEND_ACCEPTED_COUNT);
  }

  private long getSrcAppendReceivedCount(ObjectName on) throws Exception {
    return getLongAttribute(on, SRC_ATTR_APPEND_RECEVIED_COUNT);
  }

  private long getSrcEventAcceptedCount(ObjectName on) throws Exception {
    return getLongAttribute(on, SRC_ATTR_EVENT_ACCEPTED_COUNT);
  }

  private long getSrcEventReceivedCount(ObjectName on) throws Exception {
    return getLongAttribute(on, SRC_ATTR_EVENT_RECEVIED_COUNT);
  }

  private long getLongAttribute(ObjectName on, String attr) throws Exception {
    Object result = getAttribute(on, attr);
    return ((Long) result).longValue();
  }

  private Object getAttribute(ObjectName objName, String attrName)
      throws Exception {
    return mbServer.getAttribute(objName, attrName);
  }

  private String getRandomName() {
    return "random-" + System.nanoTime();
  }

}
