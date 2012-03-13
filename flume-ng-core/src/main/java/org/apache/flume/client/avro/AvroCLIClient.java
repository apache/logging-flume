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

package org.apache.flume.client.avro;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroCLIClient {

  private static final Logger logger = LoggerFactory
      .getLogger(AvroCLIClient.class);

  private static final int BATCH_SIZE = 5;

  private String hostname;
  private int port;
  private String fileName;

  private int sent;

  public static void main(String[] args) {
    AvroCLIClient client = new AvroCLIClient();

    try {
      if (client.parseCommandLine(args)) {
        client.run();
      }
    } catch (ParseException e) {
      logger.error("Unable to parse command line options - {}", e.getMessage());
    } catch (IOException e) {
      logger.error("Unable to send data to Flume - {}", e.getMessage());
      logger.debug("Exception follows.", e);
    } catch (FlumeException e) {
      logger.error("Unable to open connection to Flume. Exception follows.", e);
    } catch (EventDeliveryException e) {
      logger.error("Unable to deliver events to Flume. Exception follows.", e);
    }

    logger.debug("Exiting");
  }

  private boolean parseCommandLine(String[] args) throws ParseException {
    Options options = new Options();

    options.addOption("p", "port", true, "port of the avro source")
        .addOption("H", "host", true, "hostname of the avro source")
        .addOption("F", "filename", true, "file to stream to avro source")
        .addOption("h", "help", false, "display help text");

    CommandLineParser parser = new GnuParser();
    CommandLine commandLine = parser.parse(options, args);

    if (commandLine.hasOption('h')) {
      new HelpFormatter().printHelp("flume-ng avro-client", options, true);

      return false;
    }

    if (!commandLine.hasOption("port")) {
      throw new ParseException(
          "You must specify a port to connect to with --port");
    }

    port = Integer.parseInt(commandLine.getOptionValue("port"));

    if (!commandLine.hasOption("host")) {
      throw new ParseException(
          "You must specify a hostname to connet to with --host");
    }

    hostname = commandLine.getOptionValue("host");
    fileName = commandLine.getOptionValue("filename");

    return true;
  }

  private void run() throws IOException, FlumeException,
      EventDeliveryException {

    BufferedReader reader = null;

    RpcClient rpcClient = RpcClientFactory.getInstance(hostname, port, BATCH_SIZE);
    try {
      List<Event> eventBuffer = Lists.newArrayList();

      if (fileName != null) {
        reader = new BufferedReader(new FileReader(new File(fileName)));
      } else {
        reader = new BufferedReader(new InputStreamReader(System.in));
      }

      String line;
      long lastCheck = System.currentTimeMillis();
      long sentBytes = 0;

      int batchSize = rpcClient.getBatchSize();
      while ((line = reader.readLine()) != null) {
        // logger.debug("read:{}", line);

        int size = eventBuffer.size();
        if (size == batchSize) {
          rpcClient.appendBatch(eventBuffer);
          eventBuffer.clear();
        }

        Event event = EventBuilder.withBody(line, Charset.forName("UTF8"));
        eventBuffer.add(event);

        sentBytes += event.getBody().length;
        sent++;

        long now = System.currentTimeMillis();

        if (now >= lastCheck + 5000) {
          logger.debug("Packed {} bytes, {} events", sentBytes, sent);
          lastCheck = now;
        }
      }

      if (!eventBuffer.isEmpty()) {
        rpcClient.appendBatch(eventBuffer);
      }

      logger.debug("Finished");
    } finally {
      if (reader != null) {
        logger.debug("Closing reader");
        reader.close();
      }

      logger.debug("Closing RPC client");
      rpcClient.close();
    }
  }
}
