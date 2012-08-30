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
package org.apache.flume.channel.file;

import com.google.common.base.Charsets;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.UUID;
import org.apache.flume.Channel;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.event.EventBuilder;
import org.junit.Assert;
import static org.fest.reflect.core.Reflection.*;

public class TestUtils {

  public static FlumeEvent newPersistableEvent() {
    Map<String, String> headers = Maps.newHashMap();
    String timestamp = String.valueOf(System.currentTimeMillis());
    headers.put("timestamp", timestamp);
    FlumeEvent event = new FlumeEvent(headers,
            timestamp.getBytes());
    return event;
  }

  public static DataInput toDataInput(Writable writable) throws IOException {
    ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
    DataOutputStream dataOutput = new DataOutputStream(byteOutput);
    writable.write(dataOutput);
    ByteArrayInputStream byteInput = new ByteArrayInputStream(byteOutput.toByteArray());
    DataInputStream dataInput = new DataInputStream(byteInput);
    return dataInput;
  }

  public static void compareInputAndOut(Set<String> in, Set<String> out) {
    Assert.assertNotNull(in);
    Assert.assertNotNull(out);
    Assert.assertTrue(in.equals(out));
  }

  public static Set<String> putWithoutCommit(Channel channel, Transaction tx,
          String prefix, int number) {
    Set<String> events = Sets.newHashSet();
    tx.begin();
    for (int i = 0; i < number; i++) {
      String eventData = (prefix + UUID.randomUUID()).toString();
      Event event = EventBuilder.withBody(eventData.getBytes());
      channel.put(event);
      events.add(eventData);
    }
    return events;
  }

  public static Set<String> takeWithoutCommit(Channel channel, Transaction tx,
          int number) {
    Set<String> events = Sets.newHashSet();
    tx.begin();
    for (int i = 0; i < number; i++) {
      Event e = channel.take();
      if (e == null) {
        break;
      }
      events.add(new String(e.getBody()));
    }
    return events;
  }

  public static void forceCheckpoint(FileChannel channel) {
    Log log = field("log")
            .ofType(Log.class)
            .in(channel)
            .get();

    Assert.assertTrue("writeCheckpoint returned false",
            method("writeCheckpoint")
            .withReturnType(Boolean.class)
            .withParameterTypes(Boolean.class)
            .in(log)
            .invoke(true));
  }

  public static Set<String> takeEvents(Channel channel,
          int batchSize) throws Exception {
    return takeEvents(channel, batchSize, Integer.MAX_VALUE);
  }

  public static Set<String> takeEvents(Channel channel,
          int batchSize, int numEvents) throws Exception {
    Set<String> result = Sets.newHashSet();
    for (int i = 0; i < numEvents; i += batchSize) {
      Transaction transaction = channel.getTransaction();
      try {
        transaction.begin();
        for (int j = 0; j < batchSize; j++) {
          Event event = channel.take();
          if (event == null) {
            transaction.commit();
            return result;
          }
          result.add(new String(event.getBody(), Charsets.UTF_8));
        }
        transaction.commit();
      } catch (Exception ex) {
        transaction.rollback();
        throw ex;
      } finally {
        transaction.close();
      }

    }
    return result;
  }

  public static Set<String> putEvents(Channel channel, String prefix,
          int batchSize, int numEvents) throws Exception {
    Set<String> result = Sets.newHashSet();
    for (int i = 0; i < numEvents; i += batchSize) {
      Transaction transaction = channel.getTransaction();
      transaction.begin();
      try {
        for (int j = 0; j < batchSize; j++) {
          String s = prefix + "-" + i + "-" + j + "-" + UUID.randomUUID();
          Event event = EventBuilder.withBody(s.getBytes(Charsets.UTF_8));
          result.add(s);
          channel.put(event);
        }
        transaction.commit();
      } catch (Exception ex) {
        transaction.rollback();
        throw ex;
      } finally {
        transaction.close();
      }
    }
    return result;
  }
}
