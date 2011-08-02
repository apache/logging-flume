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
package com.cloudera.flume.handlers.endtoend;

import java.io.IOException;

import org.apache.log4j.Logger;

/**
 * This is a interface decouple event-stream extracted notifications from the
 * actions that are triggered by them. This explicitly encapsulates any
 * necessary references to FlumeNodes or FlumeControlServer/FlumeConfigMaster
 * (masters) and simplifies testing and mocking out objects.
 */
public interface AckListener {
  public void start(String group) throws IOException;

  public void end(String group) throws IOException;

  public void err(String group) throws IOException;

  public void expired(String key) throws IOException;

  public static class Empty implements AckListener {
    static Logger LOG = Logger.getLogger(Empty.class);

    @Override
    public void end(String group) throws IOException {
      LOG.info("Empty Ack Listener ended " + group);
    }

    @Override
    public void err(String group) throws IOException {
      LOG.info("Empty Ack Listener erred " + group);
    }

    @Override
    public void expired(String group) throws IOException {
      LOG.info("Empty Ack Listener expired " + group);
    }

    @Override
    public void start(String group) throws IOException {
      LOG.info("Empty Ack Listener began " + group);
    }

  }
}
