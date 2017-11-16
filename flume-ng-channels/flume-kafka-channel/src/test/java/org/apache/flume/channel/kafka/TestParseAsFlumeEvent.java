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
import org.apache.flume.Transaction;
import org.apache.flume.event.EventBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;

import static org.apache.flume.channel.kafka.KafkaChannelConfiguration.KEY_HEADER;

public class TestParseAsFlumeEvent extends TestKafkaChannelBase {

  @Test
  public void testParseAsFlumeEventFalse() throws Exception {
    doParseAsFlumeEventFalse(false);
  }

  @Test
  public void testParseAsFlumeEventFalseCheckHeader() throws Exception {
    doParseAsFlumeEventFalse(true);
  }

  @Test
  public void testParseAsFlumeEventFalseAsSource() throws Exception {
    doParseAsFlumeEventFalseAsSource(false);
  }

  @Test
  public void testParseAsFlumeEventFalseAsSourceCheckHeader() throws Exception {
    doParseAsFlumeEventFalseAsSource(true);
  }

  private void doParseAsFlumeEventFalse(Boolean checkHeaders) throws Exception {
    final KafkaChannel channel = startChannel(false);
    Properties props = channel.getProducerProps();
    KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

    for (int i = 0; i < 50; i++) {
      ProducerRecord<String, byte[]> data =
          new ProducerRecord<>(topic, String.valueOf(i) + "-header",
              String.valueOf(i).getBytes());
      producer.send(data).get();
    }
    ExecutorCompletionService<Void> submitterSvc = new
        ExecutorCompletionService<>(Executors.newCachedThreadPool());
    List<Event> events = pullEvents(channel, submitterSvc, 50, false, false);
    wait(submitterSvc, 5);
    Map<Integer, String> finals = new HashMap<>();
    for (int i = 0; i < 50; i++) {
      finals.put(Integer.parseInt(new String(events.get(i).getBody())),
          events.get(i).getHeaders().get(KEY_HEADER));
    }
    for (int i = 0; i < 50; i++) {
      Assert.assertTrue(finals.keySet().contains(i));
      if (checkHeaders) {
        Assert.assertTrue(finals.containsValue(String.valueOf(i) + "-header"));
      }
      finals.remove(i);
    }
    Assert.assertTrue(finals.isEmpty());
    channel.stop();
  }

  /**
   * Like the previous test but here we write to the channel like a Flume source would do
   * to verify that the events are written as text and not as an Avro object
   *
   * @throws Exception
   */
  private void doParseAsFlumeEventFalseAsSource(Boolean checkHeaders) throws Exception {
    final KafkaChannel channel = startChannel(false);

    List<String> msgs = new ArrayList<>();
    Map<String, String> headers = new HashMap<>();
    for (int i = 0; i < 50; i++) {
      msgs.add(String.valueOf(i));
    }
    Transaction tx = channel.getTransaction();
    tx.begin();
    for (int i = 0; i < msgs.size(); i++) {
      headers.put(KEY_HEADER, String.valueOf(i) + "-header");
      channel.put(EventBuilder.withBody(msgs.get(i).getBytes(), headers));
    }
    tx.commit();
    ExecutorCompletionService<Void> submitterSvc =
        new ExecutorCompletionService<>(Executors.newCachedThreadPool());
    List<Event> events = pullEvents(channel, submitterSvc, 50, false, false);
    wait(submitterSvc, 5);
    Map<Integer, String> finals = new HashMap<>();
    for (int i = 0; i < 50; i++) {
      finals.put(Integer.parseInt(new String(events.get(i).getBody())),
          events.get(i).getHeaders().get(KEY_HEADER));
    }
    for (int i = 0; i < 50; i++) {
      Assert.assertTrue(finals.keySet().contains(i));
      if (checkHeaders) {
        Assert.assertTrue(finals.containsValue(String.valueOf(i) + "-header"));
      }
      finals.remove(i);
    }
    Assert.assertTrue(finals.isEmpty());
    channel.stop();
  }
}
