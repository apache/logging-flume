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
package org.apache.flume.sink;

import java.util.Iterator;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.Sink.Status;
import org.apache.flume.conf.Configurable;
import org.apache.flume.lifecycle.LifecycleAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import org.apache.flume.util.OrderSelector;
import org.apache.flume.util.RandomOrderSelector;
import org.apache.flume.util.RoundRobinOrderSelector;

/**
 * <p>Provides the ability to load-balance flow over multiple sinks.</p>
 *
 * <p>The <tt>LoadBalancingSinkProcessor</tt> maintains an indexed list of
 * active sinks on which the load must be distributed. This implementation
 * supports distributing load using either via <tt>ROUND_ROBIN</tt> or via
 * <tt>RANDOM</tt> selection mechanism. The choice of selection mechanism
 * defaults to <tt>ROUND_ROBIN</tt> type, but can be overridden via
 * configuration.</p>
 *
 * <p>When invoked, this selector picks the next sink using its configured
 * selection mechanism and invokes it. In case the selected sink fails with
 * an exception, the processor picks the next available sink via its configured
 * selection mechanism. This implementation does not blacklist the failing
 * sink and instead continues to optimistically attempt every available sink.
 * If all sinks invocations result in failure, the selector propagates the
 * failure to the sink runner.</p>
 *
 * <p>
 * Sample configuration:
 * <pre>
 * {@code
 * host1.sinkgroups.group1.sinks = sink1 sink2
 * host1.sinkgroups.group1.processor.type = load_balance
 * host1.sinkgroups.group1.processor.selector = <selector type>
 * host1.sinkgroups.group1.processor.selector.selector_property = <value>
 * }
 * </pre>
 *
 * The value of processor.selector could be either <tt>round_robin</tt> for
 * round-robin scheme of load-balancing or <tt>random</tt> for random
 * selection. Alternatively you can specify your own implementation of the
 * selection algorithm by implementing the <tt>LoadBalancingSelector</tt>
 * interface. If no selector mechanism is specified, the round-robin selector
 * is used by default.
 * </p>
 * <p>
 * This implementation is not thread safe at this time
 * </p>
 *
 * @see FailoverSinkProcessor
 * @see LoadBalancingSinkProcessor.SinkSelector
 */
public class LoadBalancingSinkProcessor extends AbstractSinkProcessor {
  public static final String CONFIG_SELECTOR = "selector";
  public static final String CONFIG_SELECTOR_PREFIX = CONFIG_SELECTOR + ".";
  public static final String CONFIG_BACKOFF = "backoff";

  public static final String SELECTOR_NAME_ROUND_ROBIN = "ROUND_ROBIN";
  public static final String SELECTOR_NAME_RANDOM = "RANDOM";
  public static final String SELECTOR_NAME_ROUND_ROBIN_BACKOFF = "ROUND_ROBIN_BACKOFF";
  public static final String SELECTOR_NAME_RANDOM_BACKOFF = "RANDOM_BACKOFF";

  private static final Logger LOGGER = LoggerFactory
      .getLogger(LoadBalancingSinkProcessor.class);

  private SinkSelector selector;

  @Override
  public void configure(Context context) {
    Preconditions.checkState(getSinks().size() > 1,
        "The LoadBalancingSinkProcessor cannot be used for a single sink. "
        + "Please configure more than one sinks and try again.");

    String selectorTypeName = context.getString(CONFIG_SELECTOR,
        SELECTOR_NAME_ROUND_ROBIN);

    Boolean shouldBackOff = context.getBoolean(CONFIG_BACKOFF, false);

    selector = null;

    if (selectorTypeName.equalsIgnoreCase(SELECTOR_NAME_ROUND_ROBIN)) {
      selector = new RoundRobinSinkSelector(shouldBackOff);
    } else if (selectorTypeName.equalsIgnoreCase(SELECTOR_NAME_RANDOM)) {
      selector = new RandomOrderSinkSelector(shouldBackOff);
    } else {
      try {
        @SuppressWarnings("unchecked")
        Class<? extends SinkSelector> klass = (Class<? extends SinkSelector>)
            Class.forName(selectorTypeName);

        selector = klass.newInstance();
      } catch (Exception ex) {
        throw new FlumeException("Unable to instantiate sink selector: "
            + selectorTypeName, ex);
      }
    }

    selector.setSinks(getSinks());
    selector.configure(
        new Context(context.getSubProperties(CONFIG_SELECTOR_PREFIX)));

    LOGGER.debug("Sink selector: " + selector + " initialized");
  }

  @Override
  public void start() {
    super.start();

    selector.start();
  }

  @Override
  public void stop() {
    super.stop();

    selector.stop();
  }

  @Override
  public Status process() throws EventDeliveryException {
    Status status = null;

    Iterator<Sink> sinkIterator = selector.createSinkIterator();
    while (sinkIterator.hasNext()) {
      Sink sink = sinkIterator.next();
      try {
        status = sink.process();
        break;
      } catch (Exception ex) {
        selector.informSinkFailed(sink);
        LOGGER.warn("Sink failed to consume event. "
            + "Attempting next sink if available.", ex);
      }
    }

    if (status == null) {
      throw new EventDeliveryException("All configured sinks have failed");
    }

    return status;
  }


  /**
   * <p>
   * An interface that allows the LoadBalancingSinkProcessor to use
   * a load-balancing strategy such as round-robin, random distribution etc.
   * Implementations of this class can be plugged into the system via
   * processor configuration and are used to select a sink on every invocation.
   * </p>
   * <p>
   * An instance of the configured sink selector is create during the processor
   * configuration, its {@linkplain #setSinks(List)} method is invoked following
   * which it is configured via a subcontext. Once configured, the lifecycle of
   * this selector is tied to the lifecycle of the sink processor.
   * </p>
   * <p>
   * At runtime, the processor invokes the {@link #createSinkIterator()}
   * method for every <tt>process</tt> call to create an iteration order over
   * the available sinks. The processor then loops through this iteration order
   * until one of the sinks succeeds in processing the event. If the iterator
   * is exhausted and none of the sinks succeed, the processor will raise
   * an <tt>EventDeliveryException</tt>.
   * </p>
   */
  public interface SinkSelector extends Configurable, LifecycleAware {

    void setSinks(List<Sink> sinks);

    Iterator<Sink> createSinkIterator();

    void informSinkFailed(Sink failedSink);
  }

  /**
   * <p>A sink selector that implements the round-robin sink selection policy.
   * This implementation is not MT safe.</p>
   *
   * <p>Unfortunately both implementations need to override the base implementation
   * in AbstractSinkSelector class, because any custom sink selectors
   * will break if this stuff is moved to that class.</p>
   */
  private static class RoundRobinSinkSelector extends AbstractSinkSelector {
    private OrderSelector<Sink> selector;

    RoundRobinSinkSelector(boolean backoff) {
      selector = new RoundRobinOrderSelector<Sink>(backoff);
    }

    @Override
    public void configure(Context context) {
      super.configure(context);
      if (maxTimeOut != 0) {
        selector.setMaxTimeOut(maxTimeOut);
      }
    }

    @Override
    public Iterator<Sink> createSinkIterator() {
      return selector.createIterator();
    }

    @Override
    public void setSinks(List<Sink> sinks) {
      selector.setObjects(sinks);
    }

    @Override
    public void informSinkFailed(Sink failedSink) {
      selector.informFailure(failedSink);
    }

  }

  /**
   * A sink selector that implements a random sink selection policy. This
   * implementation is not thread safe.
   */
  private static class RandomOrderSinkSelector extends AbstractSinkSelector {

    private OrderSelector<Sink> selector;

    RandomOrderSinkSelector(boolean backoff) {
      selector = new RandomOrderSelector<Sink>(backoff);
    }

    @Override
    public void configure(Context context) {
      super.configure(context);
      if (maxTimeOut != 0) {
        selector.setMaxTimeOut(maxTimeOut);
      }
    }

    @Override
    public void setSinks(List<Sink> sinks) {
      selector.setObjects(sinks);
    }

    @Override
    public Iterator<Sink> createSinkIterator() {
      return selector.createIterator();
    }

    @Override
    public void informSinkFailed(Sink failedSink) {
      selector.informFailure(failedSink);
    }
  }
}
