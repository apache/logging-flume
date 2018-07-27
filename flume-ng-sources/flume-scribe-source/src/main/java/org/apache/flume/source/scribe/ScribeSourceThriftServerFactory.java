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

package org.apache.flume.source.scribe;

import org.apache.flume.FlumeException;
import org.apache.thrift.TException;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScribeSourceThriftServerFactory {
  private static final Logger logger =
      LoggerFactory.getLogger(ScribeSourceThriftServerFactory.class);

  public static TServer create(
      ScribeSourceConfiguration configuration, ScribeSource.Receiver receiver)
      throws FlumeException {
    if (configuration.thriftServerType == null) {
      throw new FlumeException("Initialize thrift server failed, null thriftServerType");
    }
    try {
      Scribe.Processor processor = new Scribe.Processor(receiver);
      switch (configuration.thriftServerType) {
        case THSHA_SERVER:
          return new THsHaServer(
              configuration.createTHsHasServerArg(processor, configuration.port));
        case TTHREADED_SELECTOR_SERVER:
          return new TThreadedSelectorServer(
              configuration.createTThreadedSelectorServerArgs(processor, configuration.port));
        case TTHREADPOOL_SERVER:
          return new TThreadPoolServer(
              configuration.createTThreadPoolServerArgs(processor, configuration.port));
        default:
          logger.error("Unsupported thrift server type, should happen!");
          throw new FlumeException("Unsupported thrift server type, should happen!");
      }
    } catch (FlumeException ex) {
      throw ex;
    } catch (TException ex) {
      throw new FlumeException("Create thrift server failed", ex);
    }
  }
}
