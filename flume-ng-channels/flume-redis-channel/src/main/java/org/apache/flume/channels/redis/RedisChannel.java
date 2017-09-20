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
package org.apache.flume.channels.redis;

import com.google.common.base.Optional;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.channels.redis.tools.RedisDao;
import org.apache.flume.conf.ConfigurationException;

import static org.apache.flume.channels.redis.RedisChannelConfiguration.*;

import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

public class RedisChannel extends BasicChannelSemantics {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(RedisChannel.class);

  private final Properties redisConf = new Properties();
  private final String channelUUID = UUID.randomUUID().toString();

  private AtomicReference<String> topic = new AtomicReference<String>();
  private boolean parseAsFlumeEvent = DEFAULT_PARSE_AS_FLUME_EVENT;
  private Long batch_number;
  private String queue_key;
  private RedisDao redisDao;
  private RedisChannelCounter counter;

  private final ThreadLocal<TransEvents> transFailoverController = new ThreadLocal<TransEvents>() {
    @Override
    public TransEvents initialValue() {
      return new TransEvents(channelUUID);
    }

  };

  private class TransEvents {
    final String uuid;
    final LinkedList<Event> failedEvents = new LinkedList<Event>();

    TransEvents(String uuid) {
      this.uuid = uuid;
    }
  }

  @Override
  public void start() {
    try {
      LOGGER.info("Starting Redis Channel: " + getName());
      redisDao = RedisDao.getInstance(
          redisConf.getProperty(REDIS_SERVER),
          Integer.parseInt(redisConf.getProperty(REDIS_PORT)),
          redisConf.getProperty(REDIS_PASSWORD));
      counter.start();
      super.start();
    } catch (Exception e) {
      LOGGER.error("Could not start Redis Chnnel");
      throw new FlumeException("Unable to create Redis Connections. "
          + "Check whether Redis Server are up and that the "
          + "Flume agent can connect to it.", e);
    }
  }

  @Override
  public void stop() {
    counter.stop();
    super.stop();
    LOGGER.info("Redis channel {} stopped. Metrics: {}", getName(), counter);
  }

  @Override
  protected BasicTransactionSemantics createTransaction() {
    return new RedisTransaction();
  }

  Properties getRedisConf() {
    return redisConf;
  }

  @Override
  public void configure(Context ctx) {
    String host = ctx.getString(REDIS_SERVER);
    if (host == null || host.isEmpty()) {
      throw new ConfigurationException("redis server must be specified");
    }
    String port = ctx.getString(REDIS_PORT, DEFALUT_REIDS_PORT);
    String password = ctx.getString(REDIS_PASSWORD);
    if (password == null || password.isEmpty()) {
      password = "";
      LOGGER.info("redis password has not be specified.");
    }
    String key = ctx.getString(QUEUE_KEY);
    if (key == null || key.isEmpty()) {
      throw new ConfigurationException("queue key must be specified");
    }
    String batch_number = ctx.getString(BATCH_NUMBER, DEFALUT_BATCH_NUMBER);

    redisConf.putAll(ctx.getSubProperties(RREDIS_PREFIX));
    redisConf.put(REDIS_SERVER, host);
    redisConf.put(REDIS_PORT, port);
    redisConf.put(REDIS_PASSWORD, password);
    redisConf.put(QUEUE_KEY, key);

    this.batch_number = Long.parseLong(batch_number);
    this.queue_key = key;

    parseAsFlumeEvent = ctx.getBoolean(PARSE_AS_FLUME_EVENT,
        DEFAULT_PARSE_AS_FLUME_EVENT);

    if (counter == null) {
      counter = new RedisChannelCounter(getName());
    }

  }

  private enum TransactionType {
    PUT, TAKE, NONE
  }

  private class RedisTransaction extends BasicTransactionSemantics {

    private TransactionType type = TransactionType.NONE;
    // For Puts
    private Optional<ByteArrayOutputStream> tempOutStream = Optional
        .absent();

    // For put transactions, serialize the events and batch them and send
    // it.
    private Optional<LinkedList<byte[]>> serializedEvents = Optional
        .absent();
    // For take transactions, deserialize and hold them till commit goes
    // through
    private Optional<LinkedList<Event>> events = Optional.absent();
    private Optional<SpecificDatumWriter<AvroFlumeEvent>> writer = Optional
        .absent();
    private Optional<SpecificDatumReader<AvroFlumeEvent>> reader = Optional
        .absent();

    // Fine to use null for initial value, Avro will create new ones if this
    // is null
    private BinaryEncoder encoder = null;
    private BinaryDecoder decoder = null;
    private final String batchUUID = UUID.randomUUID().toString();
    private boolean eventTaken = false;

    @Override
    protected void doPut(Event event) throws InterruptedException {
      type = TransactionType.PUT;
      if (!serializedEvents.isPresent()) {
        serializedEvents = Optional.of(new LinkedList<byte[]>());
      }

      try {
        if (!tempOutStream.isPresent()) {
          tempOutStream = Optional.of(new ByteArrayOutputStream());
        }
        if (!writer.isPresent()) {
          writer = Optional
              .of(new SpecificDatumWriter<AvroFlumeEvent>(
                  AvroFlumeEvent.class));
        }
        tempOutStream.get().reset();
        AvroFlumeEvent e = new AvroFlumeEvent(
            toCharSeqMap(event.getHeaders()), ByteBuffer.wrap(event
            .getBody()));
        encoder = EncoderFactory.get().directBinaryEncoder(
            tempOutStream.get(), encoder);
        writer.get().write(e, encoder);
        // Not really possible to avoid this copy :(
        serializedEvents.get().add(tempOutStream.get().toByteArray());
      } catch (Exception e) {
        throw new ChannelException("Error while serializing event", e);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Event doTake() throws InterruptedException {
      type = TransactionType.TAKE;
      try {
        if (!transFailoverController.get().uuid.equals(channelUUID)) {
          transFailoverController.remove();
          LOGGER.info("UUID mismatch, creating new transFailoverController");
        }
      } catch (Exception ex) {
        LOGGER.warn("Error while get multi message", ex);
      }
      if (!events.isPresent()) {
        events = Optional.of(new LinkedList<Event>());
      }
      Event e;
      if (!transFailoverController.get().failedEvents.isEmpty()) {
        e = transFailoverController.get().failedEvents.removeFirst();
      } else {
        try {
          long startTime = System.nanoTime();
          String message = redisDao.rpop(queue_key);
          while (message == null || message.isEmpty()) {
            Thread.sleep(1000);
            message = redisDao.rpop(queue_key);
          }
          long endTime = System.nanoTime();
          counter.addToRedisEventGetTimer((endTime - startTime)
              / (1000 * 1000));
          if (parseAsFlumeEvent) {
            ByteArrayInputStream in = new ByteArrayInputStream(
                message.getBytes());
            decoder = DecoderFactory.get().directBinaryDecoder(in,
                decoder);
            if (!reader.isPresent()) {
              reader = Optional
                  .of(new SpecificDatumReader<AvroFlumeEvent>(
                      AvroFlumeEvent.class));
            }
            AvroFlumeEvent event = reader.get().read(null, decoder);
            e = EventBuilder.withBody(event.getBody().array(),
                toStringMap(event.getHeaders()));
          } else {
            e = EventBuilder.withBody(message.getBytes(),
                Collections.EMPTY_MAP);
          }

        } catch (Exception ex) {
          LOGGER.warn("Error while getting events from redis", ex);
          throw new ChannelException(
              "Error while getting events from redis", ex);
        }
      }
      eventTaken = true;
      events.get().add(e);
      return e;
    }

    @Override
    protected void doCommit() throws InterruptedException {
      if (type.equals(TransactionType.NONE)) {
        return;
      }
      if (type.equals(TransactionType.PUT)) {
        try {
          List<String> messages = new ArrayList<String>();
          for (byte[] event : serializedEvents.get()) {
            messages.add(new String(event));
          }
          long startTime = System.nanoTime();
          String[] message_to_redis = new String[messages.size()];
          redisDao.lpush(queue_key,
              messages.toArray(message_to_redis));
          long endTime = System.nanoTime();
          counter.addToRedisEventSendTimer((endTime - startTime)
              / (1000 * 1000));
          counter.addToEventPutSuccessCount(Long.valueOf(messages
              .size()));
          serializedEvents.get().clear();
        } catch (Exception ex) {
          LOGGER.warn("Sending events to Kafka failed", ex);
          throw new ChannelException(
              "Commit failed as send to Kafka failed", ex);
        }
      } else {
        counter.addToEventTakeSuccessCount(Long.valueOf(events.get()
            .size()));
        events.get().clear();
      }
    }

    @Override
    protected void doRollback() throws InterruptedException {
      if (type.equals(TransactionType.NONE)) {
        return;
      }
      if (type.equals(TransactionType.PUT)) {
        serializedEvents.get().clear();
      } else {
        counter.addToRollbackCounter(Long.valueOf(events.get().size()));
        transFailoverController.get().failedEvents.addAll(events.get());
        events.get().clear();
      }
    }
  }

  /**
   * Helper function to convert a map of String to a map of CharSequence.
   */
  private static Map<CharSequence, CharSequence> toCharSeqMap(
      Map<String, String> stringMap) {
    Map<CharSequence, CharSequence> charSeqMap = new HashMap<CharSequence, CharSequence>();
    for (Map.Entry<String, String> entry : stringMap.entrySet()) {
      charSeqMap.put(entry.getKey(), entry.getValue());
    }
    return charSeqMap;
  }

  /**
   * Helper function to convert a map of CharSequence to a map of String.
   */
  private static Map<String, String> toStringMap(
      Map<CharSequence, CharSequence> charSeqMap) {
    Map<String, String> stringMap = new HashMap<String, String>();
    for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap
        .entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue()
          .toString());
    }
    return stringMap;
  }
}
