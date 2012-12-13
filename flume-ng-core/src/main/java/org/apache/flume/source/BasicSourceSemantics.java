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
package org.apache.flume.source;

import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Alternative to AbstractSource, which:
 * <ol>
 *  <li>Ensure configure cannot be called while started</li>
 *  <li>Exceptions thrown during configure, start, stop put source in ERROR state</li>
 *  <li>Exceptions thrown during start, stop will be logged but not re-thrown.</li>
 *  <li>Exception in configure disables starting</li>
 * </ol>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class BasicSourceSemantics implements Source, Configurable {
  private static final Logger logger = LoggerFactory
      .getLogger(BasicSourceSemantics.class);
  private Exception exception;
  private ChannelProcessor channelProcessor;
  private String name;
  private LifecycleState lifecycleState;

  public BasicSourceSemantics() {
    lifecycleState = LifecycleState.IDLE;
  }
  @Override
  public synchronized void configure(Context context) {
    if(isStarted()) {
      throw new IllegalStateException("Configure called when started");
    } else {
      try {
        exception = null;
        setLifecycleState(LifecycleState.IDLE);
        doConfigure(context);
      } catch (Exception e) {
        exception = e;
        setLifecycleState(LifecycleState.ERROR);
        // causes source to be removed by configuration code
        Throwables.propagate(e);
      }
    }
  }
  @Override
  public synchronized void start() {
    if (exception != null) {
      logger.error(String.format("Cannot start due to error: name = %s",
          getName()), exception);
    } else {
      try {
        Preconditions.checkState(channelProcessor != null,
            "No channel processor configured");
        doStart();
        setLifecycleState(LifecycleState.START);
      } catch (Exception e) {
        logger.error(String.format(
            "Unexpected error performing start: name = %s", getName()), e);
        exception = e;
        setLifecycleState(LifecycleState.ERROR);
      }
    }
  }
  @Override
  public synchronized void stop() {
    try {
      doStop();
      setLifecycleState(LifecycleState.STOP);
    } catch (Exception e) {
      logger.error(String.format(
          "Unexpected error performing stop: name = %s", getName()), e);
      setLifecycleState(LifecycleState.ERROR);
    }
  }
  @Override
  public synchronized void setChannelProcessor(ChannelProcessor cp) {
    channelProcessor = cp;
  }

  @Override
  public synchronized ChannelProcessor getChannelProcessor() {
    return channelProcessor;
  }
  @Override
  public synchronized void setName(String name) {
    this.name = name;
  }

  @Override
  public synchronized String getName() {
    return name;
  }

  @Override
  public synchronized LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  public String toString() {
    return this.getClass().getName() + "{name:" + name + ",state:"
        + lifecycleState +"}";
  }

  protected boolean isStarted() {
    return getLifecycleState() == LifecycleState.START;
  }
  /**
   * @return Exception thrown during configure() or start()
   */
  protected Exception getStartException() {
    return exception;
  }
  protected synchronized void setLifecycleState(LifecycleState lifecycleState) {
    this.lifecycleState = lifecycleState;
  }
  protected abstract void doConfigure(Context context) throws FlumeException;
  protected abstract void doStart() throws FlumeException;
  protected abstract void doStop() throws FlumeException;
}