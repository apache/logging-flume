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
package com.cloudera.flume.collector;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.thrift.ThriftEventSource;
import com.cloudera.flume.reporter.ReportEvent;
import com.google.common.base.Preconditions;

/**
 * This is the default collector source in a agent/collector architecture.
 * 
 * This is purposely setup as a layer of indirection between the actual
 * implementation details to may user configuration simpler. It has a default
 * options that come from flume-*.xml configuration file.
 * 
 * The actual implementation may change in the future (for example, thrift may
 * be replaced with avro) but user configurations would not need to change.
 * 
 * TODO (jon) auto version negotiation? (With agent sink)
 */
public class CollectorSource extends EventSource.Base {
  static final Logger LOG = LoggerFactory.getLogger(CollectorSource.class);

  final EventSource src;
  int port;

  public CollectorSource(int port) {
    this.src = new ThriftEventSource(port);
    this.port = port;
  }

  @Override
  public void close() throws IOException, InterruptedException {
    LOG.info("closed");
    src.close();
  }

  @Override
  public void open() throws IOException, InterruptedException {
    LOG.info("opened");
    src.open();
  }

  @Override
  public Event next() throws IOException, InterruptedException {
    Event e = src.next();
    updateEventProcessingStats(e);
    return e;
  }

  @Override
  public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
    super.getReports(namePrefix, reports);
    src.getReports(namePrefix + getName() + ".", reports);
  }

  /**
   * Get the port the collector source listens on.
   */
  public int getPort() {
    return port;
  }

  public static SourceBuilder builder() {
    return new SourceBuilder() {
      @Override
      public EventSource build(Context ctx, String... argv) {
        // accept 0 or 1 argument.
        Preconditions.checkArgument(argv.length <= 1,
            "usage: collectorSource[(port={"
                + FlumeConfiguration.COLLECTOR_EVENT_PORT + "})]");

        int port = FlumeConfiguration.get().getCollectorPort(); // default
        if (argv.length >= 1)
          port = Integer.parseInt(argv[0]); // override
        return new CollectorSource(port);
      }
    };
  }
}
