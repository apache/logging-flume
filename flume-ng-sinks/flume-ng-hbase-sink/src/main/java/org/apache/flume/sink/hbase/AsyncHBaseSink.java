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
package org.apache.flume.sink.hbase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;
import org.jboss.netty.channel.socket.nio
  .NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.stumbleupon.async.Callback;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.flume.ChannelException;
import org.apache.flume.instrumentation.SinkCounter;

/**
*
* A simple sink which reads events from a channel and writes them to HBase.
* This Sink uses an aysnchronous API internally and is likely to
* perform better.
* The Hbase configution is picked up from the first <tt>hbase-site.xml</tt>
* encountered in the classpath. This sink supports batch reading of
* events from the channel, and writing them to Hbase, to minimize the number
* of flushes on the hbase tables. To use this sink, it has to be configured
* with certain mandatory parameters:<p>
*
* <tt>table: </tt> The name of the table in Hbase to write to. <p>
* <tt>columnFamily: </tt> The column family in Hbase to write to.<p>
* Other optional parameters are:<p>
* <tt>serializer:</tt> A class implementing
*  {@link AsyncHbaseEventSerializer}.
*  An instance of
* this class will be used to serialize events which are written to hbase.<p>
* <tt>serializer.*:</tt> Passed in the <code>configure()</code> method to
* serializer
* as an object of {@link org.apache.flume.Context}.<p>
* <tt>batchSize: </tt>This is the batch size used by the client. This is the
* maximum number of events the sink will commit per transaction. The default
* batch size is 100 events.
* <p>
* <tt>timeout: </tt> The length of time in milliseconds the sink waits for
* callbacks from hbase for all events in a transaction.
* If no timeout is specified, the sink will wait forever.<p>
*
* <strong>Note: </strong> Hbase does not guarantee atomic commits on multiple
* rows. So if a subset of events in a batch are written to disk by Hbase and
* Hbase fails, the flume transaction is rolled back, causing flume to write
* all the events in the transaction all over again, which will cause
* duplicates. The serializer is expected to take care of the handling of
* duplicates etc. HBase also does not support batch increments, so if
* multiple increments are returned by the serializer, then HBase failure
* will cause them to be re-written, when HBase comes back up.
*/
public class AsyncHBaseSink extends AbstractSink implements Configurable {

  private String tableName;
  private byte[] columnFamily;
  private long batchSize;
  private static final Logger logger =
          LoggerFactory.getLogger(AsyncHBaseSink.class);
  private AsyncHbaseEventSerializer serializer;
  private String eventSerializerType;
  private Context serializerContext;
  private HBaseClient client;
  private Configuration conf;
  private Transaction txn;
  private volatile boolean open = false;
  private SinkCounter sinkCounter;
  private long timeout;
  private String zkQuorum;
  private String zkBaseDir;
  private ExecutorService sinkCallbackPool;
  private boolean isTimeoutTest;
  private boolean isCoalesceTest;
  private boolean enableWal = true;
  private boolean batchIncrements = false;
  private volatile int totalCallbacksReceived = 0;
  private Map<CellIdentifier, AtomicIncrementRequest> incrementBuffer;

  // Does not need to be thread-safe. Always called only from the sink's
  // process method.
  private final Comparator<byte[]> COMPARATOR = UnsignedBytes
    .lexicographicalComparator();

  public AsyncHBaseSink(){
    this(null);
  }

  public AsyncHBaseSink(Configuration conf) {
    this(conf, false, false);
  }

  @VisibleForTesting
  AsyncHBaseSink(Configuration conf, boolean isTimeoutTest,
    boolean isCoalesceTest) {
    this.conf = conf;
    this.isTimeoutTest = isTimeoutTest;
    this.isCoalesceTest = isCoalesceTest;
  }

  @Override
  public Status process() throws EventDeliveryException {
    /*
     * Reference to the boolean representing failure of the current transaction.
     * Since each txn gets a new boolean, failure of one txn will not affect
     * the next even if errbacks for the current txn get called while
     * the next one is being processed.
     *
     */
    if (!open) {
      throw new EventDeliveryException("Sink was never opened. " +
          "Please fix the configuration.");
    }
    AtomicBoolean txnFail = new AtomicBoolean(false);
    AtomicInteger callbacksReceived = new AtomicInteger(0);
    AtomicInteger callbacksExpected = new AtomicInteger(0);
    final Lock lock = new ReentrantLock();
    final Condition condition = lock.newCondition();
    if (incrementBuffer != null) {
      incrementBuffer.clear();
    }
    /*
     * Callbacks can be reused per transaction, since they share the same
     * locks and conditions.
     */
    Callback<Object, Object> putSuccessCallback =
            new SuccessCallback<Object, Object>(
            lock, callbacksReceived, condition);
    Callback<Object, Object> putFailureCallback =
            new FailureCallback<Object, Object>(
            lock, callbacksReceived, txnFail, condition);

    Callback<Long, Long> incrementSuccessCallback =
            new SuccessCallback<Long, Long>(
            lock, callbacksReceived, condition);
    Callback<Long, Long> incrementFailureCallback =
            new FailureCallback<Long, Long>(
            lock, callbacksReceived, txnFail, condition);

    Status status = Status.READY;
    Channel channel = getChannel();
    int i = 0;
    try {
      txn = channel.getTransaction();
      txn.begin();
      for (; i < batchSize; i++) {
        Event event = channel.take();
        if (event == null) {
          status = Status.BACKOFF;
          if (i == 0) {
            sinkCounter.incrementBatchEmptyCount();
          } else {
            sinkCounter.incrementBatchUnderflowCount();
          }
          break;
        } else {
          serializer.setEvent(event);
          List<PutRequest> actions = serializer.getActions();
          List<AtomicIncrementRequest> increments = serializer.getIncrements();
          callbacksExpected.addAndGet(actions.size());
          if (!batchIncrements) {
            callbacksExpected.addAndGet(increments.size());
          }

          for (PutRequest action : actions) {
            action.setDurable(enableWal);
            client.put(action).addCallbacks(putSuccessCallback, putFailureCallback);
          }
          for (AtomicIncrementRequest increment : increments) {
            if (batchIncrements) {
              CellIdentifier identifier = new CellIdentifier(increment.key(),
                increment.qualifier());
              AtomicIncrementRequest request
                = incrementBuffer.get(identifier);
              if (request == null) {
                incrementBuffer.put(identifier, increment);
              } else {
                request.setAmount(request.getAmount() + increment.getAmount());
              }
            } else {
              client.atomicIncrement(increment).addCallbacks(
                incrementSuccessCallback, incrementFailureCallback);
            }
          }
        }
      }
      if (batchIncrements) {
        Collection<AtomicIncrementRequest> increments = incrementBuffer.values();
        for (AtomicIncrementRequest increment : increments) {
          client.atomicIncrement(increment).addCallbacks(
            incrementSuccessCallback, incrementFailureCallback);
        }
        callbacksExpected.addAndGet(increments.size());
      }
      client.flush();
    } catch (Throwable e) {
      this.handleTransactionFailure(txn);
      this.checkIfChannelExceptionAndThrow(e);
    }
    if (i == batchSize) {
      sinkCounter.incrementBatchCompleteCount();
    }
    sinkCounter.addToEventDrainAttemptCount(i);

    lock.lock();
    long startTime = System.nanoTime();
    long timeRemaining;
    try {
      while ((callbacksReceived.get() < callbacksExpected.get())
              && !txnFail.get()) {
        timeRemaining = timeout - (System.nanoTime() - startTime);
        timeRemaining = (timeRemaining >= 0) ? timeRemaining : 0;
        try {
          if (!condition.await(timeRemaining, TimeUnit.NANOSECONDS)) {
            txnFail.set(true);
            logger.warn("HBase callbacks timed out. "
                    + "Transaction will be rolled back.");
          }
        } catch (Exception ex) {
          logger.error("Exception while waiting for callbacks from HBase.");
          this.handleTransactionFailure(txn);
          Throwables.propagate(ex);
        }
      }
    } finally {
      lock.unlock();
    }

    if (isCoalesceTest) {
      totalCallbacksReceived += callbacksReceived.get();
    }

    /*
     * At this point, either the txn has failed
     * or all callbacks received and txn is successful.
     *
     * This need not be in the monitor, since all callbacks for this txn
     * have been received. So txnFail will not be modified any more(even if
     * it is, it is set from true to true only - false happens only
     * in the next process call).
     *
     */
    if (txnFail.get()) {
      this.handleTransactionFailure(txn);
      throw new EventDeliveryException("Could not write events to Hbase. " +
          "Transaction failed, and rolled back.");
    } else {
      try {
        txn.commit();
        txn.close();
        sinkCounter.addToEventDrainSuccessCount(i);
      } catch (Throwable e) {
        this.handleTransactionFailure(txn);
        this.checkIfChannelExceptionAndThrow(e);
      }
    }

    return status;
  }

  @Override
  public void configure(Context context) {
    tableName = context.getString(HBaseSinkConfigurationConstants.CONFIG_TABLE);
    String cf = context.getString(
        HBaseSinkConfigurationConstants.CONFIG_COLUMN_FAMILY);
    batchSize = context.getLong(
        HBaseSinkConfigurationConstants.CONFIG_BATCHSIZE, new Long(100));
    serializerContext = new Context();
    //If not specified, will use HBase defaults.
    eventSerializerType = context.getString(
        HBaseSinkConfigurationConstants.CONFIG_SERIALIZER);
    Preconditions.checkNotNull(tableName,
        "Table name cannot be empty, please specify in configuration file");
    Preconditions.checkNotNull(cf,
        "Column family cannot be empty, please specify in configuration file");
    //Check foe event serializer, if null set event serializer type
    if(eventSerializerType == null || eventSerializerType.isEmpty()) {
      eventSerializerType =
          "org.apache.flume.sink.hbase.SimpleAsyncHbaseEventSerializer";
      logger.info("No serializer defined, Will use default");
    }
    serializerContext.putAll(context.getSubProperties(
            HBaseSinkConfigurationConstants.CONFIG_SERIALIZER_PREFIX));
    columnFamily = cf.getBytes(Charsets.UTF_8);
    try {
      @SuppressWarnings("unchecked")
      Class<? extends AsyncHbaseEventSerializer> clazz =
      (Class<? extends AsyncHbaseEventSerializer>)
      Class.forName(eventSerializerType);
      serializer = clazz.newInstance();
      serializer.configure(serializerContext);
      serializer.initialize(tableName.getBytes(Charsets.UTF_8), columnFamily);
    } catch (Exception e) {
      logger.error("Could not instantiate event serializer." , e);
      Throwables.propagate(e);
    }

    if(sinkCounter == null) {
      sinkCounter = new SinkCounter(this.getName());
    }
    timeout = context.getLong(HBaseSinkConfigurationConstants.CONFIG_TIMEOUT,
            HBaseSinkConfigurationConstants.DEFAULT_TIMEOUT);
    if(timeout <= 0){
      logger.warn("Timeout should be positive for Hbase sink. "
              + "Sink will not timeout.");
      timeout = HBaseSinkConfigurationConstants.DEFAULT_TIMEOUT;
    }
    //Convert to nanos.
    timeout = TimeUnit.MILLISECONDS.toNanos(timeout);

    zkQuorum = context.getString(
        HBaseSinkConfigurationConstants.ZK_QUORUM, "").trim();
    if(!zkQuorum.isEmpty()) {
      zkBaseDir = context.getString(
          HBaseSinkConfigurationConstants.ZK_ZNODE_PARENT,
          HBaseSinkConfigurationConstants.DEFAULT_ZK_ZNODE_PARENT);
    } else {
      if (conf == null) { //In tests, we pass the conf in.
        conf = HBaseConfiguration.create();
      }
      zkQuorum = ZKConfig.getZKQuorumServersString(conf);
      zkBaseDir = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    }
    Preconditions.checkState(zkQuorum != null && !zkQuorum.isEmpty(),
        "The Zookeeper quorum cannot be null and should be specified.");

    enableWal = context.getBoolean(HBaseSinkConfigurationConstants
      .CONFIG_ENABLE_WAL, HBaseSinkConfigurationConstants.DEFAULT_ENABLE_WAL);
    logger.info("The write to WAL option is set to: " + String.valueOf(enableWal));
    if(!enableWal) {
      logger.warn("AsyncHBaseSink's enableWal configuration is set to false. " +
        "All writes to HBase will have WAL disabled, and any data in the " +
        "memstore of this region in the Region Server could be lost!");
    }

    batchIncrements = context.getBoolean(
      HBaseSinkConfigurationConstants.CONFIG_COALESCE_INCREMENTS,
      HBaseSinkConfigurationConstants.DEFAULT_COALESCE_INCREMENTS);

    if(batchIncrements) {
      incrementBuffer = Maps.newHashMap();
      logger.info("Increment coalescing is enabled. Increments will be " +
        "buffered.");
    }
  }

  @VisibleForTesting
  int getTotalCallbacksReceived() {
    return totalCallbacksReceived;
  }

  @VisibleForTesting
  boolean isConfNull() {
    return conf == null;
  }
  @Override
  public void start(){
    Preconditions.checkArgument(client == null, "Please call stop "
            + "before calling start on an old instance.");
    sinkCounter.start();
    sinkCounter.incrementConnectionCreatedCount();
      sinkCallbackPool = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
        .setNameFormat(this.getName() + " HBase Call Pool").build());
    logger.info("Callback pool created");
    if(!isTimeoutTest) {
      client = new HBaseClient(zkQuorum, zkBaseDir, sinkCallbackPool);
    } else {
      client = new HBaseClient(zkQuorum, zkBaseDir,
        new NioClientSocketChannelFactory(Executors
          .newSingleThreadExecutor(),
          Executors.newSingleThreadExecutor()));
    }
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicBoolean fail = new AtomicBoolean(false);
    client.ensureTableFamilyExists(
            tableName.getBytes(Charsets.UTF_8), columnFamily).addCallbacks(
            new Callback<Object, Object>() {
              @Override
              public Object call(Object arg) throws Exception {
                latch.countDown();
                logger.info("table found");
                return null;
              }
            },
            new Callback<Object, Object>() {
              @Override
              public Object call(Object arg) throws Exception {
                fail.set(true);
                latch.countDown();
                return null;
              }
            });

    try {
      logger.info("waiting on callback");
      latch.await();
      logger.info("callback received");
    } catch (InterruptedException e) {
      sinkCounter.incrementConnectionFailedCount();
      throw new FlumeException(
          "Interrupted while waiting for Hbase Callbacks", e);
    }
    if(fail.get()){
      sinkCounter.incrementConnectionFailedCount();
      client.shutdown();
      client = null;
      throw new FlumeException(
          "Could not start sink. " +
          "Table or column family does not exist in Hbase.");
    } else {
      open = true;
    }
    client.setFlushInterval((short) 0);
    super.start();
  }

  @Override
  public void stop(){
    serializer.cleanUp();
    if (client != null) {
      client.shutdown();
    }
    sinkCounter.incrementConnectionClosedCount();
    sinkCounter.stop();

    try {
      if (sinkCallbackPool != null) {
        sinkCallbackPool.shutdown();
        if (!sinkCallbackPool.awaitTermination(5, TimeUnit.SECONDS)) {
          sinkCallbackPool.shutdownNow();
        }
      }
    } catch (InterruptedException e) {
      logger.error("Interrupted while waiting for asynchbase sink pool to " +
        "die", e);
      if (sinkCallbackPool != null) {
        sinkCallbackPool.shutdownNow();
      }
    }
    sinkCallbackPool = null;
    client = null;
    conf = null;
    open = false;
    super.stop();
  }

  private void handleTransactionFailure(Transaction txn)
      throws EventDeliveryException {
    try {
      txn.rollback();
    } catch (Throwable e) {
      logger.error("Failed to commit transaction." +
          "Transaction rolled back.", e);
      if(e instanceof Error || e instanceof RuntimeException){
        logger.error("Failed to commit transaction." +
            "Transaction rolled back.", e);
        Throwables.propagate(e);
      } else {
        logger.error("Failed to commit transaction." +
            "Transaction rolled back.", e);
        throw new EventDeliveryException("Failed to commit transaction." +
            "Transaction rolled back.", e);
      }
    } finally {
      txn.close();
    }
  }
  private class SuccessCallback<R,T> implements Callback<R,T> {
    private Lock lock;
    private AtomicInteger callbacksReceived;
    private Condition condition;
    private final boolean isTimeoutTesting;

    public SuccessCallback(Lock lck, AtomicInteger callbacksReceived,
            Condition condition) {
      lock = lck;
      this.callbacksReceived = callbacksReceived;
      this.condition = condition;
      isTimeoutTesting = isTimeoutTest;
    }

    @Override
    public R call(T arg) throws Exception {
      if (isTimeoutTesting) {
        try {
          //tests set timeout to 10 seconds, so sleep for 4 seconds
          TimeUnit.NANOSECONDS.sleep(TimeUnit.SECONDS.toNanos(4));
        } catch (InterruptedException e) {
          //ignore
        }
      }
      doCall();
      return null;
    }

    private void doCall() throws Exception {
      callbacksReceived.incrementAndGet();
      lock.lock();
      try{
        condition.signal();
      } finally {
        lock.unlock();
      }
    }
  }

  private class FailureCallback<R,T> implements Callback<R,T> {
    private Lock lock;
    private AtomicInteger callbacksReceived;
    private AtomicBoolean txnFail;
    private Condition condition;
    private final boolean isTimeoutTesting;
    public FailureCallback(Lock lck, AtomicInteger callbacksReceived,
            AtomicBoolean txnFail, Condition condition){
      this.lock = lck;
      this.callbacksReceived = callbacksReceived;
      this.txnFail = txnFail;
      this.condition = condition;
      isTimeoutTesting = isTimeoutTest;
    }

    @Override
    public R call(T arg) throws Exception {
      if (isTimeoutTesting) {
        //tests set timeout to 10 seconds, so sleep for 4 seconds
        try {
          TimeUnit.NANOSECONDS.sleep(TimeUnit.SECONDS.toNanos(4));
        } catch (InterruptedException e) {
          //ignore
        }
      }
      doCall();
      return null;
    }

    private void doCall() throws Exception {
      callbacksReceived.incrementAndGet();
      this.txnFail.set(true);
      lock.lock();
      try {
        condition.signal();
      } finally {
        lock.unlock();
      }
    }
  }

  private void checkIfChannelExceptionAndThrow(Throwable e)
          throws EventDeliveryException {
    if (e instanceof ChannelException) {
      throw new EventDeliveryException("Error in processing transaction.", e);
    } else if (e instanceof Error || e instanceof RuntimeException) {
      Throwables.propagate(e);
    }
    throw new EventDeliveryException("Error in processing transaction.", e);
  }

  private class CellIdentifier {
    private final byte[] row;
    private final byte[] column;
    private final int hashCode;
    // Since the sink operates only on one table and one cf,
    // we use the data from the owning sink
    public CellIdentifier(byte[] row, byte[] column) {
      this.row = row;
      this.column = column;
      this.hashCode =
        (Arrays.hashCode(row) * 31) * (Arrays.hashCode(column) * 31);
    }

    @Override
    public int hashCode() {
      return hashCode;
    }

    // Since we know that this class is used from only this class,
    // skip the class comparison to save time
    @Override
    public boolean equals(Object other) {
      CellIdentifier o = (CellIdentifier) other;
      if (other == null) {
        return false;
      } else {
        return (COMPARATOR.compare(row, o.row) == 0
          && COMPARATOR.compare(column, o.column) == 0);
      }
    }
  }
}
