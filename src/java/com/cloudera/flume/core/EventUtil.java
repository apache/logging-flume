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
package com.cloudera.flume.core;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class has some simple utilities for connecting sinks to sources.
 * 
 * TODO (jon) this is a place where we can do SEDA-style thread allocation if we
 * purposely want to give some sinks more CPUs etc. We actually would change
 * this from a static to a class with different versions to pick from.
 */
public class EventUtil {

  public static final Logger LOG = LoggerFactory.getLogger(EventUtil.class);

  /**
   * Dump from source to sink until source fails.
   */
  public static void dumpAll(EventSource src, EventSink snk)
      throws IOException, InterruptedException {
    while (true) {
      Event e = src.next();
      if (e == null) {
        break;
      }
      snk.append(e);
    }
  }

  public static void dumpN(long n, EventSource src, EventSink snk, boolean debug)
      throws IOException, InterruptedException {
    for (long i = 0; i < n; i++) {
      Event e = src.next();
      if (e == null) {
        LOG.info("Dumped " + i + "/" + n + " events ");
        break;
      }
      snk.append(e);

      LOG.info("dumpN: " + (i + 1) + "/" + n + " events dumped '"
          + new String(e.getBody()) + "'");
    }
  }

  /**
   * This method dumps n events from the source to the sink, and exits on
   * completion or if the source is closed/empty/done.
   */

  public static void dumpN(long n, EventSource src, EventSink snk)
      throws IOException, InterruptedException {
    dumpN(n, src, snk, false);
  }

  /*
   * Create sinks based on various command line options.
   */
  public static CommandLine defaultSinkParser(String[] argv)
      throws ParseException {
    Options options = new Options();
    options.addOption("d", false, "Directly send to collector");
    options.addOption("c", false, "Directly output to console");
    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, argv);
    return cmd;
  }

}
