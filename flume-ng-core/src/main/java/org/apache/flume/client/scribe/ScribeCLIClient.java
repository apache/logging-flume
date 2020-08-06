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

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.cli.ParseException;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ScribeCLIClient {
  private static final Logger logger = LoggerFactory
      .getLogger(ScribeCLIClient.class);

  public static void main(String[] args) {
    try {
      ScribeCLIClientConfiguration configuration = new ScribeCLIClientConfiguration();
      configuration.parseArgs(args);
      AbstractScribeClient scribeClient = createScribeClient(configuration);
      logger.info("Starting CLI client with following configuration:\n"
          + configuration.toString());
      scribeClient.run();
    } catch (ParseException e) {
      logger.error("Unable to parse command line options - {}", e.getMessage());
    } catch (IOException e) {
      logger.error("Unable to send data to Flume. Exception follows.", e);
    } catch (FlumeException e) {
      logger.error("Unable to open connection to Flume. Exception follows.", e);
    } catch (InterruptedException e) {
      logger.error("Got interrupted Exception:", e);
    } catch (ExecutionException ex) {
      logger.error("Got executionException:", ex);
    }
    logger.info("Exiting");
  }

  public static AbstractScribeClient createScribeClient(
      ScribeCLIClientConfiguration configuration) {
    switch (configuration.rpcMode) {
      case SYNC:
        return new SyncScribeClient(configuration);
      case ASYNC:
        return new AsyncScribeClient(configuration);
      default:
        return null;
    }
  }
}
