/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.solr.morphline;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kitesdk.morphline.api.Command;

/**
 * Flume sink that extracts search documents from Flume events and processes them using a morphline
 * {@link Command} chain.
 */
public class MorphlineSink extends AbstractSink implements Configurable {

  private int maxBatchSize = 1000;
  private long maxBatchDurationMillis = 1000;
  private String handlerClass;
  private MorphlineHandler handler;
  private Context context;
  private SinkCounter sinkCounter;

  public static final String BATCH_SIZE = "batchSize";
  public static final String BATCH_DURATION_MILLIS = "batchDurationMillis";
  public static final String HANDLER_CLASS = "handlerClass";
  
  private static final Logger LOGGER = LoggerFactory.getLogger(MorphlineSink.class);

  public MorphlineSink() {
    this(null);
  }

  /** For testing only */
  protected MorphlineSink(MorphlineHandler handler) {
    this.handler = handler;
  }

  @Override
  public void configure(Context context) {
    this.context = context;
    maxBatchSize = context.getInteger(BATCH_SIZE, maxBatchSize);
    maxBatchDurationMillis = context.getLong(BATCH_DURATION_MILLIS, maxBatchDurationMillis);
    handlerClass = context.getString(HANDLER_CLASS, MorphlineHandlerImpl.class.getName());    
    if (sinkCounter == null) {
      sinkCounter = new SinkCounter(getName());
    }
  }

  /**
   * Returns the maximum number of events to take per flume transaction;
   * override to customize
   */
  private int getMaxBatchSize() {
    return maxBatchSize;
  }

  /** Returns the maximum duration per flume transaction; override to customize */
  private long getMaxBatchDurationMillis() {
    return maxBatchDurationMillis;
  }

  @Override
  public synchronized void start() {
    LOGGER.info("Starting Morphline Sink {} ...", this);
    sinkCounter.start();
    if (handler == null) {
      MorphlineHandler tmpHandler;
      try {
        tmpHandler = (MorphlineHandler) Class.forName(handlerClass).newInstance();
      } catch (Exception e) {
        throw new ConfigurationException(e);
      }
      tmpHandler.configure(context);
      handler = tmpHandler;
    }    
    super.start();
    LOGGER.info("Morphline Sink {} started.", getName());
  }

  @Override
  public synchronized void stop() {
    LOGGER.info("Morphline Sink {} stopping...", getName());
    try {
      if (handler != null) {
        handler.stop();
      }
      sinkCounter.stop();
      LOGGER.info("Morphline Sink {} stopped. Metrics: {}, {}", getName(), sinkCounter);
    } finally {
      super.stop();
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    int batchSize = getMaxBatchSize();
    long batchEndTime = System.currentTimeMillis() + getMaxBatchDurationMillis();
    Channel myChannel = getChannel();
    Transaction txn = myChannel.getTransaction();
    txn.begin();
    boolean isMorphlineTransactionCommitted = true;
    try {
      int numEventsTaken = 0;
      handler.beginTransaction();
      isMorphlineTransactionCommitted = false;

      // repeatedly take and process events from the Flume queue
      for (int i = 0; i < batchSize; i++) {
        Event event = myChannel.take();
        if (event == null) {
          break;
        }
        sinkCounter.incrementEventDrainAttemptCount();
        numEventsTaken++;
        LOGGER.debug("Flume event: {}", event);
        //StreamEvent streamEvent = createStreamEvent(event);
        handler.process(event);
        if (System.currentTimeMillis() >= batchEndTime) {
          break;
        }
      }

      // update metrics
      if (numEventsTaken == 0) {
        sinkCounter.incrementBatchEmptyCount();
      }
      if (numEventsTaken < batchSize) {
        sinkCounter.incrementBatchUnderflowCount();
      } else {
        sinkCounter.incrementBatchCompleteCount();
      }
      handler.commitTransaction();
      isMorphlineTransactionCommitted = true;
      txn.commit();
      sinkCounter.addToEventDrainSuccessCount(numEventsTaken);
      return numEventsTaken == 0 ? Status.BACKOFF : Status.READY;
    } catch (Throwable t) {
      // Ooops - need to rollback and back off
      LOGGER.error("Morphline Sink " + getName() + ": Unable to process event from channel " + myChannel.getName()
            + ". Exception follows.", t);
      try {
        if (!isMorphlineTransactionCommitted) {
          handler.rollbackTransaction();
        }
      } catch (Throwable t2) {
        LOGGER.error("Morphline Sink " + getName() + ": Unable to rollback morphline transaction. " +
        		"Exception follows.", t2);
      } finally {
        try {
          txn.rollback();
        } catch (Throwable t4) {
          LOGGER.error("Morphline Sink " + getName() + ": Unable to rollback Flume transaction. " +
              "Exception follows.", t4);
        }
      }

      if (t instanceof Error) {
        throw (Error) t; // rethrow original exception
      } else if (t instanceof ChannelException) {
        return Status.BACKOFF;
      } else {
        throw new EventDeliveryException("Failed to send events", t); // rethrow and backoff
      }
    } finally {
      txn.close();
    }
  }
  
  @Override
  public String toString() {
    int i = getClass().getName().lastIndexOf('.') + 1;
    String shortClassName = getClass().getName().substring(i);
    return getName() + " (" + shortClassName + ")";
  }

}
