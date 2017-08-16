/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.source.kafka;

import com.google.common.base.Charsets;
import junit.framework.Assert;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flume.source.kafka.KafkaSourceConstants.AVRO_EVENT;
import static org.apache.flume.source.kafka.KafkaSourceConstants.BATCH_DURATION_MS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.BATCH_SIZE;
import static org.apache.flume.source.kafka.KafkaSourceConstants.BOOTSTRAP_SERVERS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.KAFKA_CONSUMER_PREFIX;
import static org.apache.flume.source.kafka.KafkaSourceConstants.PARTITION_HEADER;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TIMESTAMP_HEADER;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TOPICS;
import static org.apache.flume.source.kafka.KafkaSourceConstants.TOPIC_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestKafkaSourceProcessing extends TestKafkaSourceBase {

  @SuppressWarnings("unchecked")
  @Test
  public void testOffsets() throws InterruptedException, EventDeliveryException {
    long batchDuration = 2000;
    context.put(TOPICS, topic1);
    context.put(BATCH_DURATION_MS,
        String.valueOf(batchDuration));
    context.put(BATCH_SIZE, "3");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);
    PollableSource.Status status = kafkaSource.process();
    assertEquals(PollableSource.Status.BACKOFF, status);
    assertEquals(0, events.size());
    kafkaServer.produce(topic1, "", "record1");
    kafkaServer.produce(topic1, "", "record2");
    Thread.sleep(500L);
    status = kafkaSource.process();
    assertEquals(PollableSource.Status.READY, status);
    assertEquals(2, events.size());
    events.clear();
    kafkaServer.produce(topic1, "", "record3");
    kafkaServer.produce(topic1, "", "record4");
    kafkaServer.produce(topic1, "", "record5");
    Thread.sleep(500L);
    assertEquals(PollableSource.Status.READY, kafkaSource.process());
    assertEquals(3, events.size());
    assertEquals("record3", new String(events.get(0).getBody(), Charsets.UTF_8));
    assertEquals("record4", new String(events.get(1).getBody(), Charsets.UTF_8));
    assertEquals("record5", new String(events.get(2).getBody(), Charsets.UTF_8));
    events.clear();
    kafkaServer.produce(topic1, "", "record6");
    kafkaServer.produce(topic1, "", "record7");
    kafkaServer.produce(topic1, "", "record8");
    kafkaServer.produce(topic1, "", "record9");
    kafkaServer.produce(topic1, "", "record10");
    Thread.sleep(500L);
    assertEquals(PollableSource.Status.READY, kafkaSource.process());
    assertEquals(3, events.size());
    assertEquals("record6", new String(events.get(0).getBody(), Charsets.UTF_8));
    assertEquals("record7", new String(events.get(1).getBody(), Charsets.UTF_8));
    assertEquals("record8", new String(events.get(2).getBody(), Charsets.UTF_8));
    events.clear();
    kafkaServer.produce(topic1, "", "record11");
    // status must be READY due to time out exceed.
    assertEquals(PollableSource.Status.READY, kafkaSource.process());
    assertEquals(3, events.size());
    assertEquals("record9", new String(events.get(0).getBody(), Charsets.UTF_8));
    assertEquals("record10", new String(events.get(1).getBody(), Charsets.UTF_8));
    assertEquals("record11", new String(events.get(2).getBody(), Charsets.UTF_8));
    events.clear();
    kafkaServer.produce(topic1, "", "record12");
    kafkaServer.produce(topic1, "", "record13");
    // stop kafka source
    kafkaSource.stop();
    // start again
    kafkaSource = new KafkaSource();
    kafkaSource.setChannelProcessor(createGoodChannel());
    kafkaSource.configure(context);
    kafkaSource.start();
    kafkaServer.produce(topic1, "", "record14");
    Thread.sleep(1000L);
    assertEquals(PollableSource.Status.READY, kafkaSource.process());
    assertEquals(3, events.size());
    assertEquals("record12", new String(events.get(0).getBody(), Charsets.UTF_8));
    assertEquals("record13", new String(events.get(1).getBody(), Charsets.UTF_8));
    assertEquals("record14", new String(events.get(2).getBody(), Charsets.UTF_8));
    events.clear();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testProcessItNotEmpty() throws EventDeliveryException,
      SecurityException, NoSuchFieldException, IllegalArgumentException,
      IllegalAccessException, InterruptedException {
    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE, "1");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    kafkaServer.produce(topic0, "", "hello, world");

    Thread.sleep(500L);
    Assert.assertEquals(PollableSource.Status.READY, kafkaSource.process());
    Assert.assertEquals(PollableSource.Status.BACKOFF, kafkaSource.process());
    Assert.assertEquals(1, events.size());

    Assert.assertEquals("hello, world", new String(events.get(0).getBody(),
        Charsets.UTF_8));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testProcessItNotEmptyBatch() throws EventDeliveryException,
      SecurityException, NoSuchFieldException, IllegalArgumentException,
      IllegalAccessException, InterruptedException {
    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE,"2");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    kafkaServer.produce(topic0, "", "hello, world");
    kafkaServer.produce(topic0, "", "foo, bar");

    Thread.sleep(500L);

    PollableSource.Status status = kafkaSource.process();
    assertEquals(PollableSource.Status.READY, status);
    Assert.assertEquals("hello, world", new String(events.get(0).getBody(),
        Charsets.UTF_8));
    Assert.assertEquals("foo, bar", new String(events.get(1).getBody(),
        Charsets.UTF_8));

  }


  @SuppressWarnings("unchecked")
  @Test
  public void testProcessItEmpty() throws EventDeliveryException,
      SecurityException, NoSuchFieldException, IllegalArgumentException,
      IllegalAccessException, InterruptedException {
    context.put(TOPICS, topic0);
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    PollableSource.Status status = kafkaSource.process();
    assertEquals(PollableSource.Status.BACKOFF, status);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNonExistingTopic() throws EventDeliveryException,
      SecurityException, NoSuchFieldException, IllegalArgumentException,
      IllegalAccessException, InterruptedException {
    context.put(TOPICS,"faketopic");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    PollableSource.Status status = kafkaSource.process();
    assertEquals(PollableSource.Status.BACKOFF, status);
  }

  @SuppressWarnings("unchecked")
  @Test(expected = FlumeException.class)
  public void testNonExistingKafkaServer() throws EventDeliveryException, SecurityException,
      NoSuchFieldException, IllegalArgumentException,
      IllegalAccessException, InterruptedException {
    context.put(TOPICS, topic0);
    context.put(BOOTSTRAP_SERVERS,"blabla:666");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    PollableSource.Status status = kafkaSource.process();
    assertEquals(PollableSource.Status.BACKOFF, status);
  }

  @Test
  public void testBatchTime() throws InterruptedException, EventDeliveryException {
    context.put(TOPICS, topic0);
    context.put(BATCH_DURATION_MS, "250");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    for (int i = 1; i < 5000; i++) {
      kafkaServer.produce(topic0, "", "hello, world " + i);
    }
    Thread.sleep(500L);

    long error = 50;
    long startTime = System.currentTimeMillis();
    PollableSource.Status status = kafkaSource.process();
    long endTime = System.currentTimeMillis();
    assertEquals(PollableSource.Status.READY, status);
    assertTrue(endTime - startTime < (context.getLong(BATCH_DURATION_MS) + error));
  }

  // Consume event, stop source, start again and make sure we are not
  // consuming same event again
  @Test
  public void testCommit() throws InterruptedException, EventDeliveryException {
    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE, "1");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    kafkaServer.produce(topic0, "", "hello, world");

    Thread.sleep(500L);

    Assert.assertEquals(PollableSource.Status.READY, kafkaSource.process());
    kafkaSource.stop();
    Thread.sleep(500L);
    kafkaSource.start();
    Thread.sleep(500L);
    Assert.assertEquals(PollableSource.Status.BACKOFF, kafkaSource.process());
  }

  // Remove channel processor and test if we can consume events again
  @Test
  public void testNonCommit() throws EventDeliveryException, InterruptedException {
    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE,"1");
    context.put(BATCH_DURATION_MS,"30000");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    kafkaServer.produce(topic0, "", "hello, world");
    Thread.sleep(500L);

    kafkaSource.setChannelProcessor(createBadChannel());
    log.debug("processing from kafka to bad channel");
    Assert.assertEquals(PollableSource.Status.BACKOFF, kafkaSource.process());

    log.debug("repairing channel");
    kafkaSource.setChannelProcessor(createGoodChannel());

    log.debug("re-process to good channel - this should work");
    kafkaSource.process();
    Assert.assertEquals("hello, world", new String(events.get(0).getBody(), Charsets.UTF_8));
  }

  @Test
  public void testTwoBatches() throws InterruptedException, EventDeliveryException {
    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE,"1");
    context.put(BATCH_DURATION_MS, "30000");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    kafkaServer.produce(topic0, "", "event 1");
    Thread.sleep(500L);

    kafkaSource.process();
    Assert.assertEquals("event 1", new String(events.get(0).getBody(), Charsets.UTF_8));
    events.clear();

    kafkaServer.produce(topic0, "", "event 2");
    Thread.sleep(500L);
    kafkaSource.process();
    Assert.assertEquals("event 2", new String(events.get(0).getBody(), Charsets.UTF_8));
  }

  @Test
  public void testTwoBatchesWithAutocommit() throws InterruptedException, EventDeliveryException {
    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE,"1");
    context.put(BATCH_DURATION_MS,"30000");
    context.put(KAFKA_CONSUMER_PREFIX + "enable.auto.commit", "true");
    kafkaSource.configure(context);
    kafkaSource.start();
    Thread.sleep(500L);

    kafkaServer.produce(topic0, "", "event 1");
    Thread.sleep(500L);

    kafkaSource.process();
    Assert.assertEquals("event 1", new String(events.get(0).getBody(), Charsets.UTF_8));
    events.clear();

    kafkaServer.produce(topic0, "", "event 2");
    Thread.sleep(500L);
    kafkaSource.process();
    Assert.assertEquals("event 2", new String(events.get(0).getBody(), Charsets.UTF_8));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testNullKey() throws EventDeliveryException, SecurityException, NoSuchFieldException,
      IllegalArgumentException, IllegalAccessException,
      InterruptedException {
    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE, "1");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    kafkaServer.produce(topic0, null, "hello, world");

    Thread.sleep(500L);

    Assert.assertEquals(PollableSource.Status.READY, kafkaSource.process());
    Assert.assertEquals(PollableSource.Status.BACKOFF, kafkaSource.process());
    Assert.assertEquals(1, events.size());

    Assert.assertEquals("hello, world", new String(events.get(0).getBody(), Charsets.UTF_8));
  }



  @Test
  public void testAvroEvent() throws InterruptedException, EventDeliveryException, IOException {
    SpecificDatumWriter<AvroFlumeEvent> writer;
    ByteArrayOutputStream tempOutStream;
    BinaryEncoder encoder;

    context.put(TOPICS, topic0);
    context.put(BATCH_SIZE, "1");
    context.put(AVRO_EVENT, "true");
    kafkaSource.configure(context);
    kafkaSource.start();

    Thread.sleep(500L);

    tempOutStream = new ByteArrayOutputStream();
    writer = new SpecificDatumWriter<AvroFlumeEvent>(AvroFlumeEvent.class);

    Map<CharSequence, CharSequence> headers = new HashMap<CharSequence,CharSequence>();
    headers.put("header1", "value1");
    headers.put("header2", "value2");

    AvroFlumeEvent e = new AvroFlumeEvent(headers, ByteBuffer.wrap("hello, world".getBytes()));
    encoder = EncoderFactory.get().directBinaryEncoder(tempOutStream, null);
    writer.write(e, encoder);
    encoder.flush();

    kafkaServer.produce(topic0, "", tempOutStream.toByteArray());

    String currentTimestamp = Long.toString(System.currentTimeMillis());

    headers.put(TIMESTAMP_HEADER, currentTimestamp);
    headers.put(PARTITION_HEADER, "1");
    headers.put(TOPIC_HEADER, "topic0");

    e = new AvroFlumeEvent(headers, ByteBuffer.wrap("hello, world2".getBytes()));
    tempOutStream.reset();
    encoder = EncoderFactory.get().directBinaryEncoder(tempOutStream, null);
    writer.write(e, encoder);
    encoder.flush();

    kafkaServer.produce(topic0, "", tempOutStream.toByteArray());

    Thread.sleep(500L);
    Assert.assertEquals(PollableSource.Status.READY, kafkaSource.process());
    Assert.assertEquals(PollableSource.Status.READY, kafkaSource.process());
    Assert.assertEquals(PollableSource.Status.BACKOFF, kafkaSource.process());

    Assert.assertEquals(2, events.size());

    Event event = events.get(0);

    Assert.assertEquals("hello, world", new String(event.getBody(), Charsets.UTF_8));

    Assert.assertEquals("value1", e.getHeaders().get("header1"));
    Assert.assertEquals("value2", e.getHeaders().get("header2"));


    event = events.get(1);

    Assert.assertEquals("hello, world2", new String(event.getBody(), Charsets.UTF_8));

    Assert.assertEquals("value1", e.getHeaders().get("header1"));
    Assert.assertEquals("value2", e.getHeaders().get("header2"));
    Assert.assertEquals(currentTimestamp, e.getHeaders().get(TIMESTAMP_HEADER));
    Assert.assertEquals(e.getHeaders().get(PARTITION_HEADER), "1");
    Assert.assertEquals(e.getHeaders().get(TOPIC_HEADER),"topic0");

  }


}
