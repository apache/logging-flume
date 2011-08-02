/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.master.availability;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.BackOffFailOverSink;
import com.cloudera.flume.core.CompositeSink;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.util.BackoffPolicy;
import com.cloudera.util.CappedExponentialBackoff;
import com.google.common.base.Preconditions;

/**
 * This sink takes a list of hosts/ips to fall back onto. It generates a chain
 * of lazyopening failover nodes
 */
public class FailoverChainSink extends EventSink.Base {
  final static Logger LOG = Logger.getLogger(FailoverChainSink.class);

  final EventSink snk;

  public FailoverChainSink(Context context, String specFormat, List<String> list,
      BackoffPolicy backoff) throws FlumeSpecException {
    snk = buildSpec(context, specFormat, list, backoff);
  }

  public FailoverChainSink(Context context, String specFormat,
      List<String> list, long initialBackoff, long maxBackoff)
      throws FlumeSpecException {
    snk =
        buildSpec(context, specFormat, list, new CappedExponentialBackoff(
            initialBackoff, maxBackoff));
  }

  /**
   * This current version requires a "%s" that gets replaced with the value from
   * the list.
   * 
   * Warning! this is a potential security problem.
   */
  static EventSink buildSpec(Context context, String spec, List<String> list,
      BackoffPolicy policy) throws FlumeSpecException {

    // iterate through the list backwards
    EventSink cur = null;
    for (int i = list.size() - 1; i >= 0; i--) {
      String s = list.get(i);
      // this should be a composite sink.
      String failoverSpec = String.format(spec, s);
      LOG.debug("failover spec is : " + failoverSpec);
      EventSink tsnk = new CompositeSink(context, failoverSpec);
      if (cur == null) {
        cur = tsnk;
        continue;
      }
      cur = new BackOffFailOverSink(tsnk, cur, policy);
    }
    return cur;
  }

  @Override
  public void append(Event e) throws IOException {
    snk.append(e);
    super.append(e);
  }

  @Override
  public void close() throws IOException {
    snk.close();
  }

  @Override
  public void open() throws IOException {
    snk.open();
  }

  @Override
  public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
    super.getReports(namePrefix, reports);
    snk.getReports(namePrefix + getName() + ".", reports);
  }

  public static SinkBuilder builder() {
    return new SinkBuilder() {
      @Override
      public EventSink build(Context context, String... argv) {
        Preconditions.checkArgument(argv.length >= 2);
        String format = argv[0];
        FlumeConfiguration conf = FlumeConfiguration.get();
        List<String> list = Arrays.asList(argv);
        list = list.subList(1, list.size());

        try {
          return new FailoverChainSink(context, format, list, conf
              .getFailoverInitialBackoff(), conf.getFailoverMaxBackoff());
        } catch (FlumeSpecException e) {
          LOG.error("Bad spec or format", e);
          throw new IllegalArgumentException(e);
        }
      }
    };
  }
}
