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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.serialization.EventSerializer;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.stumbleupon.async.Callback;

public class AsyncHBaseSink extends AbstractSink implements Configurable {

  private String tableName;
  private byte[] columnFamily;
  private long batchSize;
  private CounterGroup counterGroup = new CounterGroup();
  private static final Logger logger = LoggerFactory.getLogger(HBaseSink.class);
  private AsyncHbaseEventSerializer serializer;
  private String eventSerializerType;
  private Context serializerContext;
  private HBaseClient client;
  private Configuration conf;
  private Transaction txn;

  public AsyncHBaseSink(){
    conf = HBaseConfiguration.create();
  }

  public AsyncHBaseSink(Configuration conf) {
    this.conf = conf;
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
    AtomicBoolean txnFail = new AtomicBoolean(false);
    Status status = Status.READY;
    Channel channel = getChannel();
    txn = channel.getTransaction();
    txn.begin();
    List<PutRequest> actions = new LinkedList<PutRequest>();
    List<AtomicIncrementRequest> increments =
        new LinkedList<AtomicIncrementRequest>();
    for(int i = 0; i < batchSize; i++){
      Event event = channel.take();
      if(event == null){
        status = Status.BACKOFF;
        counterGroup.incrementAndGet("channel.underflow");
        break;
      } else {
        serializer.setEvent(event);
        actions.addAll(serializer.getActions());
        increments.addAll(serializer.getIncrements());
      }
    }
    CountDownLatch latch =
        new CountDownLatch(actions.size() + increments.size());
    for(PutRequest action : actions) {
      Callback<Object, Object> callback =
          new SuccessCallback<Object, Object>(latch);
      Callback<Object, Object> errback =
          new ErrBack<Object, Object>(latch, txnFail);
      client.put(action).addCallbacks(callback, errback);
    }
    for(AtomicIncrementRequest increment : increments) {
      Callback<Long, Long> callback =
          new SuccessCallback<Long, Long>(latch);
      Callback<Long, Long> errback = new ErrBack<Long, Long>(latch, txnFail);
      client.atomicIncrement(increment).addCallbacks(callback, errback);
    }

    try {
      latch.await();
    } catch (InterruptedException e1) {
      this.handleTransactionFailure(txn);
      throw new EventDeliveryException("Sink interrupted while waiting" +
          "for Hbase callbacks. Exception follows.", e1);
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
    } else {
      try{
        txn.commit();
      } catch (Throwable e) {
        try{
          txn.rollback();
        } catch (Exception e2) {
          logger.error("Exception in rollback. Rollback might not have been" +
              "successful." , e2);
        }
        counterGroup.incrementAndGet("transaction.rollback");
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

    return status;
  }

  @Override
  public void configure(Context context) {
    tableName = context.getString("table");
    String cf = context.getString("columnFamily");
    batchSize = context.getLong("batchSize", new Long(100));
    serializerContext = new Context();
    //If not specified, will use HBase defaults.
    eventSerializerType = context.getString(
        "serializer");
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
    serializerContext.putAll(
        context.getSubProperties(EventSerializer.CTX_PREFIX));
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
  }
  @Override
  public void start(){
    Preconditions.checkArgument(client == null, "Please call stop " +
        "before calling start on an old instance.");
    String zkQuorum = conf.get(HConstants.ZOOKEEPER_QUORUM);
    String zkBaseDir = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT);
    if(zkBaseDir != null){
      client = new HBaseClient(zkQuorum, zkBaseDir);
    } else {
      client = new HBaseClient(zkQuorum);
    }
    client.setFlushInterval((short) 0);
    super.start();
  }

  @Override
  public void stop(){
    client.shutdown();
    client = null;
  }

  private void handleTransactionFailure(Transaction txn)
      throws EventDeliveryException {
    try {
      txn.rollback();
    } catch (Throwable e) {
      counterGroup.incrementAndGet("transaction.rollback");
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

  private class SuccessCallback<R, T> implements Callback<R, T> {

    private CountDownLatch latch;
    public SuccessCallback(CountDownLatch latch){
      this.latch = latch;
    }

    @Override
    public R call(T arg0) throws Exception {
      latch.countDown();
      return null;
    }
  }

  private class ErrBack<R, T> implements Callback<R, T> {

    private CountDownLatch latch;
    /*
     * Reference to the boolean representing failure of the current transaction.
     * Since each txn gets a new boolean, failure of one txn will not affect
     * the next even if errbacks for the current txn come while the next one is
     * being processed.
     *
     */
    private AtomicBoolean txnFail;
    public ErrBack(CountDownLatch latch, AtomicBoolean txnFail){
      this.latch = latch;
      this.txnFail = txnFail;
    }

    @Override
    public R call(T arg0) throws Exception {
      /*
       * getCount() and countDown are thread safe. countDown will not let
       * count to go < 0 anyway.
       * So even if multiple threads call this method simultaneously,
       * it is ok - eventually one will call countDown and set count to 0,
       * then all countDown calls are simply no-ops anyway, and the
       * process thread is released at count == 0.
       */
      txnFail.set(true);
      while(latch.getCount() > 0 ) {
        latch.countDown();
      }
      return null;
    }

  }
}
