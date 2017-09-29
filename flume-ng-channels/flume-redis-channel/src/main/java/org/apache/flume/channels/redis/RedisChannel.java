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
import org.apache.flume.channels.redis.tools.RedisInit;
import org.apache.flume.channels.redis.tools.RedisOperator;
import org.apache.flume.conf.ConfigurationException;

import static org.apache.flume.channels.redis.RedisChannelConfiguration.*;

import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPoolConfig;

import com.google.common.net.HostAndPort;

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

  private boolean parseAsFlumeEvent = DEFAULT_PARSE_AS_FLUME_EVENT;
  private String queue_key;
  private RedisOperator redisOperator;
  private JedisPoolConfig jedisPoolConfig;
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
      redisOperator = RedisInit.getInstance(redisConf, jedisPoolConfig);
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

  private JedisPoolConfig createJedisConfig(Properties redisConf)
      throws ConfigurationException {
    String maxTotal = redisConf.getProperty(MAX_TO_TOTAL, DEFAULT_MAX_TO_TOTAL);
    String maxIdle = redisConf.getProperty(MAX_IDLE, DEFAULT_MAX_IDLE);
    String minIdle = redisConf.getProperty(MIN_IDLE, DEFAULT_MIN_IDLE);
    String maxWaitMillis = redisConf.getProperty(MAX_WAIT_MILLIS,
        DEFAULT_MAX_WAIT_MILLIS);
    String testOnBorrow = redisConf.getProperty(TEST_ON_BORROW,
        DEFAULT_TEST_ON_BORROW);
    String testOnReturn = redisConf.getProperty(TEST_ON_RETURN,
        DEFAULT_TEST_ON_RETURN);
    String testWhileIdle = redisConf.getProperty(TEST_WHILE_IDLE,
        DEFAULT_TEST_WHILE_IDLE);
    String timeBetweenEvictionRunsMillis = redisConf.getProperty(
        TIME_BETWEEN_EVICTION_RUNMILLIS,
        DEFAULT_TIME_BETWEEN_EVICTION_RUNMILLIS);
    String numTestsPerEvictionRun = redisConf.getProperty(
        NUM_TESTS_PER_EVICTION_RUN, DEFAULT_NUM_TESTS_PER_EVICTION_RUN);
    String minEvictableIdleTimeMillis = redisConf.getProperty(
        MIN_EVICTABLE_IDLE_TIME_MILLS, DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLS);

    if (!StringUtils.isNumeric(maxTotal) || !StringUtils.isNumeric(maxIdle)
        || !StringUtils.isNumeric(minIdle)
        || !StringUtils.isNumeric(maxWaitMillis)
        || !StringUtils.isNumeric(timeBetweenEvictionRunsMillis)
        || !StringUtils.isNumeric(numTestsPerEvictionRun)
        || !StringUtils.isNumeric(minEvictableIdleTimeMillis)) {
      StringBuilder sb = new StringBuilder(
          "Error configuration of redis, the value of these key must be a postive number, but you" +
              " may specified other types.\n");
      sb.append(MAX_TO_TOTAL + ": " + maxTotal + "\n");
      sb.append(MAX_IDLE + ": " + maxIdle + "\n");
      sb.append(MIN_IDLE + ": " + minIdle + "\n");
      sb.append(MAX_WAIT_MILLIS + ": " + maxWaitMillis + "\n");
      sb.append(TIME_BETWEEN_EVICTION_RUNMILLIS + ": "
          + timeBetweenEvictionRunsMillis + "\n");
      sb.append(
          NUM_TESTS_PER_EVICTION_RUN + ": " + numTestsPerEvictionRun + "\n");
      sb.append(MIN_EVICTABLE_IDLE_TIME_MILLS + ": "
          + minEvictableIdleTimeMillis + "\n");
      throw new ConfigurationException(sb.toString());
    }
    boolean testOnBorrowBool = true;
    boolean testOnReturnBool = true;
    boolean testWhileIdleBool = true;
    try {
      testOnBorrowBool = Boolean.parseBoolean(testOnBorrow.trim());
      testOnReturnBool = Boolean.parseBoolean(testOnReturn.trim());
      testWhileIdleBool = Boolean.parseBoolean(testWhileIdle.trim());
    } catch (Exception ex) {
      StringBuilder sb = new StringBuilder(
          "Error configuration of redis, the value of these key must be with \"true\" or " +
              "\"false\", but you may specified other types.\n");
      sb.append(TEST_ON_BORROW + ": " + testOnBorrow + "\n");
      sb.append(testOnReturnBool + ": " + testOnReturnBool + "\n");
      sb.append(testWhileIdleBool + ": " + testWhileIdleBool + "\n");
      throw new ConfigurationException(sb.toString());
    }

    JedisPoolConfig jedisConfig = new JedisPoolConfig();
    jedisConfig.setMaxTotal(Integer.parseInt(maxTotal));
    jedisConfig.setMaxIdle(Integer.parseInt(maxIdle));
    jedisConfig.setMinIdle(Integer.parseInt(minIdle));
    jedisConfig.setMaxWaitMillis(Integer.parseInt(maxWaitMillis));
    jedisConfig.setTestOnBorrow(testOnBorrowBool);
    jedisConfig.setTestOnReturn(testOnReturnBool);
    jedisConfig.setTestWhileIdle(testWhileIdleBool);
    jedisConfig.setTimeBetweenEvictionRunsMillis(
        Integer.parseInt(timeBetweenEvictionRunsMillis));
    jedisConfig
        .setNumTestsPerEvictionRun(Integer.parseInt(numTestsPerEvictionRun));
    jedisConfig.setMinEvictableIdleTimeMillis(
        Integer.parseInt(minEvictableIdleTimeMillis));
    return jedisConfig;
  }

  @Override
  public void configure(Context ctx) {
    String redisType = ctx.getString(SERVER_TYPE, DEFAULT_SEVER_TYPE);
    if (!redisType.equals(DEFAULT_SEVER_TYPE)
        && !redisType.equals(CLUSTER_SERVER_TYPE)
        && !redisType.equals(SENTINEL_SERVER_TYPE)) {
      throw new ConfigurationException("redis type must be specified with \""
          + DEFAULT_SEVER_TYPE + "\", \"" + CLUSTER_SERVER_TYPE + "\" or \""
          + SENTINEL_SERVER_TYPE + "\". but I get a \"" + redisType + "\".");
    }
    String host;
    if (redisType.equals(DEFAULT_SEVER_TYPE)) {
      host = ctx.getString(REDIS_SERVER);
      if (null == host || host.isEmpty()) {
        throw new ConfigurationException("redis server must be specified.");
      }
      String hostAndPortPair[] = host.split(":");
      if (2 != hostAndPortPair.length
          || !StringUtils.isNumeric(hostAndPortPair[1])) {
        throw new ConfigurationException("wrong redis server format.");
      }
    } else if (redisType.equals(CLUSTER_SERVER_TYPE)) {
      host = ctx.getString(REDIS_CLUSTER_SERVER);
      if (null == host || host.isEmpty()) {
        throw new ConfigurationException(
            "redis cluster server must be specified.");
      }
      String[] hosts = host.split(",");
      if (0 == hosts.length) {
        throw new ConfigurationException("wrong redis cluster server format.");
      }
      for (String hostAndPort : hosts) {
        String[] hostAndPortPair = hostAndPort.split(":");
        if (2 != hostAndPortPair.length
            || !StringUtils.isNumeric(hostAndPortPair[1])) {
          throw new ConfigurationException(
              "wrong redis server format with host: " + hostAndPort + ".");
        }
      }
    } else if (redisType.equals(SENTINEL_SERVER_TYPE)) {
      host = ctx.getString(REDIS_SENTINEL_SERVER);
      if (null == host || host.isEmpty()) {
        throw new ConfigurationException(
            "redis sentinel server must be specified.");
      }
      String masterName = ctx.getString(SENTINEL_MASTER_NAME);
      if (null == masterName || masterName.isEmpty()) {
        throw new ConfigurationException(
            "Sentinel master name must be specified.");
      }
      redisConf.put(SENTINEL_MASTER_NAME, masterName);
    } else {
      throw new ConfigurationException(
          "Unknown error about redis type \"" + redisType + "\".");
    }
    String batchNumber = ctx.getString(BATCH_NUMBER, DEFAULT_BATCH_NUMBER);
    String password = ctx.getString(REDIS_PASSWORD);
    if (null == password || password.isEmpty()) {
      password = "";
      LOGGER.info("redis password has not be specified.");
    }
    String key = ctx.getString(QUEUE_KEY);
    if (key == null || key.isEmpty()) {
      throw new ConfigurationException("queue key must be specified");
    }
    String redisTimeOut = redisConf.getProperty(REDIS_CONNTIMOUT,
        DEFAULT_REDIS_CONNTIMEOUT);
    String clusterMaxAttemp = redisConf.getProperty(CLUSTER_MAX_ATTEMP,
        DEFAULT_CLUSTER_MAX_ATTEMP);
    if (!StringUtils.isNumeric(redisTimeOut)
        || !StringUtils.isNumeric(clusterMaxAttemp)) {
      StringBuilder sb = new StringBuilder(
          "Error configuration of redis, the value of these key must be a postive number, but you" +
              " may specified other types.\n");
      sb.append(REDIS_CONNTIMOUT + ": " + redisTimeOut + "\n");
      sb.append(CLUSTER_MAX_ATTEMP + ": " + clusterMaxAttemp + "\n");
      throw new ConfigurationException(sb.toString());
    }
    redisConf.putAll(ctx.getSubProperties(RREDIS_PREFIX));
    redisConf.put(SERVER_TYPE, redisType);
    redisConf.put(REDIS_SERVER, host);
    redisConf.put(REDIS_PASSWORD, password);
    redisConf.put(QUEUE_KEY, key);
    redisConf.put(REDIS_CONNTIMOUT, redisTimeOut);
    redisConf.put(CLUSTER_MAX_ATTEMP, clusterMaxAttemp);
    redisConf.put(BATCH_NUMBER, batchNumber);

    this.jedisPoolConfig = createJedisConfig(redisConf);
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
    private ByteArrayOutputStream tempOutStream = new ByteArrayOutputStream();

    // For put transactions, serialize the events and batch them and send
    // it.
    private LinkedList<byte[]> serializedEvents = new LinkedList<byte[]>();
    // For take transactions, deserialize and hold them till commit goes
    // through
    private LinkedList<Event> events = new LinkedList<Event>();
    private SpecificDatumWriter<AvroFlumeEvent> writer = new SpecificDatumWriter<AvroFlumeEvent>(
        AvroFlumeEvent.class);
    private SpecificDatumReader<AvroFlumeEvent> reader = new SpecificDatumReader<AvroFlumeEvent>(
        AvroFlumeEvent.class);

    // Fine to use null for initial value, Avro will create new ones if this
    // is null
    private BinaryEncoder encoder = null;
    private BinaryDecoder decoder = null;
    private final String batchUUID = UUID.randomUUID().toString();
    private boolean eventTaken = false;

    @Override
    protected void doPut(Event event) throws InterruptedException {
      type = TransactionType.PUT;
      try {
        tempOutStream.reset();
        AvroFlumeEvent e = new AvroFlumeEvent(toCharSeqMap(event.getHeaders()),
            ByteBuffer.wrap(event.getBody()));
        encoder = EncoderFactory.get().directBinaryEncoder(tempOutStream,
            encoder);
        writer.write(e, encoder);
        // Not really possible to avoid this copy :(
        serializedEvents.add(tempOutStream.toByteArray());
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
      Event e;
      if (!transFailoverController.get().failedEvents.isEmpty()) {
        e = transFailoverController.get().failedEvents.removeFirst();
      } else {
        try {
          long startTime = System.nanoTime();
          Long queueLength = redisOperator.llen(queue_key);
          while (queueLength == null || queueLength == 0) {
            Thread.sleep(1000);
            queueLength = redisOperator.llen(queue_key);
          }
          long endTime = System.nanoTime();
          counter
              .addToRedisEventGetTimer((endTime - startTime) / (1000 * 1000));
          String message = redisOperator.rpop(queue_key);
          if (parseAsFlumeEvent) {
            ByteArrayInputStream in = new ByteArrayInputStream(
                message.getBytes());
            decoder = DecoderFactory.get().directBinaryDecoder(in, decoder);
            AvroFlumeEvent event = reader.read(null, decoder);
            e = EventBuilder.withBody(event.getBody().array(),
                toStringMap(event.getHeaders()));
          } else {
            e = EventBuilder.withBody(message.getBytes(),
                Collections.EMPTY_MAP);
          }

        } catch (Exception ex) {
          LOGGER.warn("Error while getting events from redis", ex);
          throw new ChannelException("Error while getting events from redis",
              ex);
        }
      }
      eventTaken = true;
      events.add(e);
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
          for (byte[] event : serializedEvents) {
            messages.add(new String(event));
          }
          long startTime = System.nanoTime();
          String[] message_to_redis = new String[messages.size()];
          redisOperator.lpush(queue_key, messages.toArray(message_to_redis));
          long endTime = System.nanoTime();
          counter
              .addToRedisEventSendTimer((endTime - startTime) / (1000 * 1000));
          counter.addToEventPutSuccessCount(Long.valueOf(messages.size()));
          serializedEvents.clear();
        } catch (Exception ex) {
          LOGGER.warn("Sending events to Kafka failed", ex);
          throw new ChannelException("Commit failed as send to Kafka failed",
              ex);
        }
      } else {
        counter.addToEventTakeSuccessCount(Long.valueOf(events.size()));
        events.clear();
      }
    }

    @Override
    protected void doRollback() throws InterruptedException {
      if (type.equals(TransactionType.NONE)) {
        return;
      }
      if (type.equals(TransactionType.PUT)) {
        serializedEvents.clear();
      } else {
        counter.addToRollbackCounter(Long.valueOf(events.size()));
        transFailoverController.get().failedEvents.addAll(events);
        events.clear();
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
    for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
      stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
  }
}
