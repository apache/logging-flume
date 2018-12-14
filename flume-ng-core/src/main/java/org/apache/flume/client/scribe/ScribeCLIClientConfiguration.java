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

package org.apache.flume.client.scribe;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;

public class ScribeCLIClientConfiguration {
  public String server;
  public int port;
  public int ioDepth;
  public int epm;
  public long eventNumberToSend;
  public int eventLength;
  public String categoryName;
  public int rpcBatchSize;
  public RpcMode rpcMode;
  public long runTimeSec;

  public static final String PORT_FLAG = "port";
  public static final String SERVER_FLAG = "server";
  public static final String IO_DEPTH_FLAG = "iodepth";
  public static final String EVENT_NUM_TO_SEND_FLAG = "eventnumtosend";
  public static final String EVENT_LENGTH_FLAG = "eventlength";
  public static final String CATEGORY_NAME_FLAG = "categoryname";
  public static final String BATCH_RPC_SIZE_FLAG = "batchrpcsize";
  public static final String RPC_MODE_FLAG = "rpcmode";
  public static final String HELP_FLAG = "help";
  public static final String RUN_TIME_SECONDS_FLAG = "runtimeseconds";
  public static final String EPM_FLAG = "eventsperminute";

  public static final int DEFAULT_IO_DEPTH = 1;
  public static final long DEFAULT_EVENT_TO_SEND = Long.MAX_VALUE;
  public static final int DEFAULT_EVENT_LENGTH = 0;
  public static final String DEFAULT_CATEGORY_NAME = "ScribeCLIClientDefaultCategoryName";
  public static final RpcMode DEFAULT_RPC_MODE = RpcMode.SYNC;
  public static final long DEFAULT_RUN_TIME_SEC = 0;
  public static final int DEFAULT_RPC_BATCH_SIZE = 10;
  public static final int DEFAULT_EPM = 1000000;

  public enum RpcMode {
    SYNC,
    ASYNC
  }

  public boolean parseArgs(String[] args) throws ParseException {
    Options options = new Options();
    options
        .addOption("p", PORT_FLAG, true, "port of the scribe source")
        .addOption("s", SERVER_FLAG, true, "hostname of the scribe source")
        .addOption("q", IO_DEPTH_FLAG, true,
            "means thread number in syncing mode, queue depth with async mode")
        .addOption("e", EPM_FLAG, true,
            "number of events per minute per thread, work for async only now")
        .addOption("n", EVENT_NUM_TO_SEND_FLAG, true,
            "manually specified number of events")
        .addOption("l", EVENT_LENGTH_FLAG, true, "event size")
        .addOption("c", CATEGORY_NAME_FLAG, true,
            "category name, default ScribeCLIClientDefaultCategoryName")
        .addOption("b", BATCH_RPC_SIZE_FLAG, true,
            "batch size, default 10")
        .addOption("r", RPC_MODE_FLAG, true,
            "specify sync/async rpc, default sync mode")
        .addOption("h", HELP_FLAG, false, "display help text")
        .addOption("t", RUN_TIME_SECONDS_FLAG, true, "CLI client run time in secs");

    CommandLineParser parser = new GnuParser();
    CommandLine commandLine = parser.parse(options, args);

    if (commandLine.hasOption(HELP_FLAG)) {
      new HelpFormatter().printHelp("flume-ng scribe-client", "", options,
          "Each thread emits e (events per minute) spread across"
              + " the minute at 10s interval. Default ioDepth is 1 "
              + "and default epm is 1M. Use <Ctrl+c> to stop program", true);
      return false;
    }

    if ((!commandLine.hasOption(SERVER_FLAG)) || (!commandLine.hasOption(PORT_FLAG))) {
      throw new ParseException(
          "You must specify both --server and --port");
    }
    server = commandLine.getOptionValue(SERVER_FLAG);
    port = Integer.parseInt(commandLine.getOptionValue(PORT_FLAG));

    ioDepth = (commandLine.hasOption(IO_DEPTH_FLAG)) ?
        Integer.parseInt(commandLine.getOptionValue(IO_DEPTH_FLAG)) : DEFAULT_IO_DEPTH;
    epm = (commandLine.hasOption(EPM_FLAG)) ?
        Integer.parseInt(commandLine.getOptionValue(EPM_FLAG)) : DEFAULT_EPM;
    eventNumberToSend = (commandLine.hasOption(EVENT_NUM_TO_SEND_FLAG)) ?
        Long.parseLong(commandLine.getOptionValue(EVENT_NUM_TO_SEND_FLAG)) : DEFAULT_EVENT_TO_SEND;
    eventLength = (commandLine.hasOption(EVENT_LENGTH_FLAG)) ?
        Integer.parseInt(commandLine.getOptionValue(EVENT_LENGTH_FLAG)) : DEFAULT_EVENT_LENGTH;
    categoryName = commandLine.hasOption(CATEGORY_NAME_FLAG) ?
        commandLine.getOptionValue(CATEGORY_NAME_FLAG) : DEFAULT_CATEGORY_NAME;
    rpcBatchSize = (commandLine.hasOption(BATCH_RPC_SIZE_FLAG)) ?
        Integer.parseInt(commandLine.getOptionValue(BATCH_RPC_SIZE_FLAG)) : DEFAULT_RPC_BATCH_SIZE;
    rpcMode = commandLine.hasOption(RPC_MODE_FLAG) ?
        RpcMode.valueOf(StringUtils.upperCase(commandLine.getOptionValue(RPC_MODE_FLAG)))
        : DEFAULT_RPC_MODE;
    runTimeSec = (commandLine.hasOption(RUN_TIME_SECONDS_FLAG)) ?
        Long.parseLong(commandLine.getOptionValue(RUN_TIME_SECONDS_FLAG)) : DEFAULT_RUN_TIME_SEC;
    return true;
  }

  @Override
  public String toString() {
    return "ScribeCLIClientConfiguration server:" + server + ", port:"
        + port + ", ioDepth:" + ioDepth + ", epm:" + epm
        + ", eventNumberToSend:" + eventNumberToSend
        + ", eventLength:" + eventLength + ", categoryName:" + categoryName
        + ", rpcBatchSize:" + rpcBatchSize + ", rpcMode:" + rpcMode
        + ", runTimeSec:" + runTimeSec;
  }
}
