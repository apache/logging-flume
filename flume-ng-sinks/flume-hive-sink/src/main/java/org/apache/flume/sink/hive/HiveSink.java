/**
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

package org.apache.flume.sink.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.hive.hcatalog.streaming.HiveEndPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HiveSink extends AbstractSink implements Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(HiveSink.class);

  private static final int DEFAULT_MAXOPENCONNECTIONS = 500;
  private static final int DEFAULT_TXNSPERBATCH = 100;
  private static final int DEFAULT_BATCHSIZE = 15000;
  private static final int DEFAULT_CALLTIMEOUT = 10000;
  private static final int DEFAULT_IDLETIMEOUT = 0;
  private static final int DEFAULT_HEARTBEATINTERVAL = 240; // seconds

  private Map<HiveEndPoint, HiveWriter> allWriters;

  private SinkCounter sinkCounter;
  private volatile int idleTimeout;
  private String metaStoreUri;
  private String proxyUser;
  private String database;
  private String table;
  private List<String> partitionVals;
  private Integer txnsPerBatchAsk;
  private Integer batchSize;
  private Integer maxOpenConnections;
  private boolean autoCreatePartitions;
  private String serializerType;
  private HiveEventSerializer serializer;

  /**
   * Default timeout for blocking I/O calls in HiveWriter
   */
  private Integer callTimeout;
  private Integer heartBeatInterval;

  private ExecutorService callTimeoutPool;

  private boolean useLocalTime;
  private TimeZone timeZone;
  private boolean needRounding;
  private int roundUnit;
  private Integer roundValue;

  private Timer heartBeatTimer = new Timer();
  private AtomicBoolean timeToSendHeartBeat = new AtomicBoolean(false);

  @VisibleForTesting
  Map<HiveEndPoint, HiveWriter> getAllWriters() {
    return allWriters;
  }

  // read configuration and setup thresholds
  @Override
  public void configure(Context context) {

    metaStoreUri = context.getString(Config.HIVE_METASTORE);
    if (metaStoreUri == null) {
      throw new IllegalArgumentException(Config.HIVE_METASTORE + " config setting is not " +
              "specified for sink " + getName());
    }
    if (metaStoreUri.equalsIgnoreCase("null")) { // for testing support
      metaStoreUri = null;
    }
    proxyUser = null; // context.getString("hive.proxyUser"); not supported by hive api yet
    database = context.getString(Config.HIVE_DATABASE);
    if (database == null) {
      throw new IllegalArgumentException(Config.HIVE_DATABASE + " config setting is not " +
            "specified for sink " + getName());
    }
    table = context.getString(Config.HIVE_TABLE);
    if (table == null) {
      throw new IllegalArgumentException(Config.HIVE_TABLE + " config setting is not " +
              "specified for sink " + getName());
    }

    String partitions = context.getString(Config.HIVE_PARTITION);
    if (partitions != null) {
      partitionVals = Arrays.asList(partitions.split(","));
    }


    txnsPerBatchAsk = context.getInteger(Config.HIVE_TXNS_PER_BATCH_ASK, DEFAULT_TXNSPERBATCH);
    if (txnsPerBatchAsk < 0) {
      LOG.warn(getName() + ". hive.txnsPerBatchAsk must be  positive number. Defaulting to "
              + DEFAULT_TXNSPERBATCH);
      txnsPerBatchAsk = DEFAULT_TXNSPERBATCH;
    }
    batchSize = context.getInteger(Config.BATCH_SIZE, DEFAULT_BATCHSIZE);
    if (batchSize < 0) {
      LOG.warn(getName() + ". batchSize must be  positive number. Defaulting to "
              + DEFAULT_BATCHSIZE);
      batchSize = DEFAULT_BATCHSIZE;
    }
    idleTimeout = context.getInteger(Config.IDLE_TIMEOUT, DEFAULT_IDLETIMEOUT);
    if (idleTimeout < 0) {
      LOG.warn(getName() + ". idleTimeout must be  positive number. Defaulting to "
              + DEFAULT_IDLETIMEOUT);
      idleTimeout = DEFAULT_IDLETIMEOUT;
    }
    callTimeout = context.getInteger(Config.CALL_TIMEOUT, DEFAULT_CALLTIMEOUT);
    if (callTimeout < 0) {
      LOG.warn(getName() + ". callTimeout must be  positive number. Defaulting to "
              + DEFAULT_CALLTIMEOUT);
      callTimeout = DEFAULT_CALLTIMEOUT;
    }

    heartBeatInterval = context.getInteger(Config.HEART_BEAT_INTERVAL, DEFAULT_HEARTBEATINTERVAL);
    if (heartBeatInterval < 0) {
      LOG.warn(getName() + ". heartBeatInterval must be  positive number. Defaulting to "
              + DEFAULT_HEARTBEATINTERVAL);
      heartBeatInterval = DEFAULT_HEARTBEATINTERVAL;
    }
    maxOpenConnections = context.getInteger(Config.MAX_OPEN_CONNECTIONS,
                                            DEFAULT_MAXOPENCONNECTIONS);
    autoCreatePartitions =  context.getBoolean("autoCreatePartitions", true);

    // Timestamp processing
    useLocalTime = context.getBoolean(Config.USE_LOCAL_TIME_STAMP, false);

    String tzName = context.getString(Config.TIME_ZONE);
    timeZone = (tzName == null) ? null : TimeZone.getTimeZone(tzName);
    needRounding = context.getBoolean(Config.ROUND, false);

    String unit = context.getString(Config.ROUND_UNIT, Config.MINUTE);
    if (unit.equalsIgnoreCase(Config.HOUR)) {
      this.roundUnit = Calendar.HOUR_OF_DAY;
    } else if (unit.equalsIgnoreCase(Config.MINUTE)) {
      this.roundUnit = Calendar.MINUTE;
    } else if (unit.equalsIgnoreCase(Config.SECOND)) {
      this.roundUnit = Calendar.SECOND;
    } else {
      LOG.warn(getName() + ". Rounding unit is not valid, please set one of " +
              "minute, hour or second. Rounding will be disabled");
      needRounding = false;
    }
    this.roundValue = context.getInteger(Config.ROUND_VALUE, 1);
    if (roundUnit == Calendar.SECOND || roundUnit == Calendar.MINUTE) {
      Preconditions.checkArgument(roundValue > 0 && roundValue <= 60,
              "Round value must be > 0 and <= 60");
    } else if (roundUnit == Calendar.HOUR_OF_DAY) {
      Preconditions.checkArgument(roundValue > 0 && roundValue <= 24,
              "Round value must be > 0 and <= 24");
    }

    // Serializer
    serializerType = context.getString(Config.SERIALIZER, "");
    if (serializerType.isEmpty()) {
      throw new IllegalArgumentException("serializer config setting is not " +
              "specified for sink " + getName());
    }

    serializer = createSerializer(serializerType);
    serializer.configure(context);

    Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");

    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }

  @VisibleForTesting
  protected SinkCounter getCounter() {
    return sinkCounter;
  }
  private HiveEventSerializer createSerializer(String serializerName)  {
    if (serializerName.compareToIgnoreCase(HiveDelimitedTextSerializer.ALIAS) == 0 ||
        serializerName.compareTo(HiveDelimitedTextSerializer.class.getName()) == 0) {
      return new HiveDelimitedTextSerializer();
    } else if (serializerName.compareToIgnoreCase(HiveJsonSerializer.ALIAS) == 0 ||
            serializerName.compareTo(HiveJsonSerializer.class.getName()) == 0) {
      return new HiveJsonSerializer();
    }

    try {
      return (HiveEventSerializer) Class.forName(serializerName).newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to instantiate serializer: " + serializerName
              + " on sink: " + getName(), e);
    }
  }


  /**
   * Pull events out of channel, find corresponding HiveWriter and write to it.
   * Take at most batchSize events per Transaction. <br/>
   * This method is not thread safe.
   */
  public Status process() throws EventDeliveryException {
    // writers used in this Txn

    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    transaction.begin();
    boolean success = false;
    try {
      // 1 Enable Heart Beats
      if (timeToSendHeartBeat.compareAndSet(true, false)) {
        enableHeartBeatOnAllWriters();
      }

      // 2 Drain Batch
      int txnEventCount = drainOneBatch(channel);
      transaction.commit();
      success = true;

      // 3 Update Counters
      if (txnEventCount < 1) {
        return Status.BACKOFF;
      } else {
        return Status.READY;
      }
    } catch (InterruptedException err) {
      LOG.warn(getName() + ": Thread was interrupted.", err);
      return Status.BACKOFF;
    } catch (Exception e) {
      throw new EventDeliveryException(e);
    } finally {
      if (!success) {
        transaction.rollback();
      }
      transaction.close();
    }
  }

  // Drains one batch of events from Channel into Hive
  private int drainOneBatch(Channel channel)
          throws HiveWriter.Failure, InterruptedException {
    int txnEventCount = 0;
    try {
      Map<HiveEndPoint,HiveWriter> activeWriters = Maps.newHashMap();
      for (; txnEventCount < batchSize; ++txnEventCount) {
        // 0) Read event from Channel
        Event event = channel.take();
        if (event == null) {
          break;
        }

        //1) Create end point by substituting place holders
        HiveEndPoint endPoint = makeEndPoint(metaStoreUri, database, table,
                partitionVals, event.getHeaders(), timeZone,
                needRounding, roundUnit, roundValue, useLocalTime);

        //2) Create or reuse Writer
        HiveWriter writer = getOrCreateWriter(activeWriters, endPoint);

        //3) Write
        LOG.debug("{} : Writing event to {}", getName(), endPoint);
        writer.write(event);

      } // for

      //4) Update counters
      if (txnEventCount == 0) {
        sinkCounter.incrementBatchEmptyCount();
      } else if (txnEventCount == batchSize) {
        sinkCounter.incrementBatchCompleteCount();
      } else {
        sinkCounter.incrementBatchUnderflowCount();
      }
      sinkCounter.addToEventDrainAttemptCount(txnEventCount);


      // 5) Flush all Writers
      for (HiveWriter writer : activeWriters.values()) {
        writer.flush(true);
      }

      sinkCounter.addToEventDrainSuccessCount(txnEventCount);
      return txnEventCount;
    } catch (HiveWriter.Failure e) {
      // in case of error we close all TxnBatches to start clean next time
      LOG.warn(getName() + " : " + e.getMessage(), e);
      abortAllWriters();
      closeAllWriters();
      throw e;
    }
  }

  private void enableHeartBeatOnAllWriters() {
    for (HiveWriter writer : allWriters.values()) {
      writer.setHearbeatNeeded();
    }
  }

  private HiveWriter getOrCreateWriter(Map<HiveEndPoint, HiveWriter> activeWriters,
                                       HiveEndPoint endPoint)
          throws HiveWriter.ConnectException, InterruptedException {
    try {
      HiveWriter writer = allWriters.get( endPoint );
      if (writer == null) {
        LOG.info(getName() + ": Creating Writer to Hive end point : " + endPoint);
        writer = new HiveWriter(endPoint, txnsPerBatchAsk, autoCreatePartitions,
                callTimeout, callTimeoutPool, proxyUser, serializer, sinkCounter);

        sinkCounter.incrementConnectionCreatedCount();
        if (allWriters.size() > maxOpenConnections) {
          int retired = closeIdleWriters();
          if (retired == 0) {
            closeEldestWriter();
          }
        }
        allWriters.put(endPoint, writer);
        activeWriters.put(endPoint, writer);
      } else {
        if (activeWriters.get(endPoint) == null)  {
          activeWriters.put(endPoint,writer);
        }
      }
      return writer;
    } catch (HiveWriter.ConnectException e) {
      sinkCounter.incrementConnectionFailedCount();
      throw e;
    }

  }

  private HiveEndPoint makeEndPoint(String metaStoreUri, String database, String table,
                                    List<String> partVals, Map<String, String> headers,
                                    TimeZone timeZone, boolean needRounding,
                                    int roundUnit, Integer roundValue,
                                    boolean useLocalTime)  {
    if (partVals == null) {
      return new HiveEndPoint(metaStoreUri, database, table, null);
    }

    ArrayList<String> realPartVals = Lists.newArrayList();
    for (String partVal : partVals) {
      realPartVals.add(BucketPath.escapeString(partVal, headers, timeZone,
              needRounding, roundUnit, roundValue, useLocalTime));
    }
    return new HiveEndPoint(metaStoreUri, database, table, realPartVals);
  }

  /**
   * Locate writer that has not been used for longest time and retire it
   */
  private void closeEldestWriter() throws InterruptedException {
    long oldestTimeStamp = System.currentTimeMillis();
    HiveEndPoint eldest = null;
    for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
      if (entry.getValue().getLastUsed() < oldestTimeStamp) {
        eldest = entry.getKey();
        oldestTimeStamp = entry.getValue().getLastUsed();
      }
    }

    try {
      sinkCounter.incrementConnectionCreatedCount();
      LOG.info(getName() + ": Closing least used Writer to Hive EndPoint : " + eldest);
      allWriters.remove(eldest).close();
    } catch (InterruptedException e) {
      LOG.warn(getName() + ": Interrupted when attempting to close writer for end point: "
              + eldest, e);
      throw e;
    }
  }

  /**
   * Locate all writers past idle timeout and retire them
   * @return number of writers retired
   */
  private int closeIdleWriters() throws InterruptedException {
    int count = 0;
    long now = System.currentTimeMillis();
    ArrayList<HiveEndPoint> retirees = Lists.newArrayList();

    //1) Find retirement candidates
    for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
      if (now - entry.getValue().getLastUsed() > idleTimeout) {
        ++count;
        retirees.add(entry.getKey());
      }
    }
    //2) Retire them
    for (HiveEndPoint ep : retirees) {
      sinkCounter.incrementConnectionClosedCount();
      LOG.info(getName() + ": Closing idle Writer to Hive end point : {}", ep);
      allWriters.remove(ep).close();
    }
    return count;
  }

  /**
   * Closes all writers and remove them from cache
   * @return number of writers retired
   */
  private void closeAllWriters() throws InterruptedException {
    //1) Retire writers
    for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
      entry.getValue().close();
    }

    //2) Clear cache
    allWriters.clear();
  }

  /**
   * Abort current Txn on all writers
   * @return number of writers retired
   */
  private void abortAllWriters() throws InterruptedException {
    for (Entry<HiveEndPoint,HiveWriter> entry : allWriters.entrySet()) {
      entry.getValue().abort();
    }
  }

  @Override
  public void stop() {
    // do not constrain close() calls with a timeout
    for (Entry<HiveEndPoint, HiveWriter> entry : allWriters.entrySet()) {
      try {
        HiveWriter w = entry.getValue();
        w.close();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
      }
    }

    // shut down all thread pools
    callTimeoutPool.shutdown();
    try {
      while (callTimeoutPool.isTerminated() == false) {
        callTimeoutPool.awaitTermination(
              Math.max(DEFAULT_CALLTIMEOUT, callTimeout), TimeUnit.MILLISECONDS);
      }
    } catch (InterruptedException ex) {
      LOG.warn(getName() + ":Shutdown interrupted on " + callTimeoutPool, ex);
    }

    callTimeoutPool = null;
    allWriters.clear();
    allWriters = null;
    sinkCounter.stop();
    super.stop();
    LOG.info("Hive Sink {} stopped", getName() );
  }

  @Override
  public void start() {
    String timeoutName = "hive-" + getName() + "-call-runner-%d";
    // call timeout pool needs only 1 thd as sink is effectively single threaded
    callTimeoutPool = Executors.newFixedThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat(timeoutName).build());

    this.allWriters = Maps.newHashMap();
    sinkCounter.start();
    super.start();
    setupHeartBeatTimer();
    LOG.info(getName() + ": Hive Sink {} started", getName() );
  }

  private void setupHeartBeatTimer() {
    if (heartBeatInterval > 0) {
      heartBeatTimer.schedule(new TimerTask() {
        @Override
        public void run() {
          timeToSendHeartBeat.set(true);
          setupHeartBeatTimer();
        }
      }, heartBeatInterval * 1000);
    }
  }


  @Override
  public String toString() {
    return "{ Sink type:" + getClass().getSimpleName() + ", name:" + getName() +
            " }";
  }

}
