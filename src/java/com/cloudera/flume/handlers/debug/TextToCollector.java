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
package com.cloudera.flume.handlers.debug;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.handlers.thrift.ThriftEventSink;
import com.cloudera.util.Benchmark;

/**
 * This is a simple program that takes a list of textfiles and shoots if off to
 * a collector host/port
 * 
 * This does benchmark info
 * @deprecated
 */
public class TextToCollector {
  static void core(CommandLine cmd) throws IOException {
    String[] argv = cmd.getArgs();

    Benchmark b = new Benchmark();

    b.mark("init");
    FlumeConfiguration conf = FlumeConfiguration.get();
    ThriftEventSink tes = new ThriftEventSink(conf.getCollectorHost(), conf
        .getCollectorPort(), false);
    tes.open();

    MemorySinkSource mem = cmd.hasOption("m") ? new MemorySinkSource() : null;

    for (String f : argv) {
      EventSource src = null;
      if (cmd.hasOption("l")) {
        b.mark("log4jtext");
        src = new Log4jTextFileSource(f);
      } else if (cmd.hasOption("t")) {
        b.mark("random access text");
        src = new TextFileSource(f);
      } else {
        b.mark("buffered reader text");
        src = new TextReaderSource(f);
      }
      src.open();

      b.mark("fileread");
      if (mem != null) {

        EventUtil.dumpAll(src, mem);
      } else {
        EventUtil.dumpAll(src, tes);
      }
      src.close();
    }

    b.mark("memdump");
    if (mem != null) {
      EventUtil.dumpAll(mem, tes);
    }

    b.mark("done");

    b.done();
    tes.close();

  }

  static public void main(String[] argv) throws IOException {

    Options opts = new Options();
    opts.addOption("m", false, "Load events into memory");
    opts.addOption("t", false, "Simple text file format loader");
    opts.addOption("l", false, "Log4j text file format loader");

    try {
      if (argv.length < 1) {
        HelpFormatter fmt = new HelpFormatter();
        fmt.printHelp("TextToCollector", opts, true);
        System.exit(-1);
      }

      CommandLineParser parser = new PosixParser();
      CommandLine cmd = parser.parse(opts, argv);

      core(cmd);
    } catch (ParseException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

  }
}
