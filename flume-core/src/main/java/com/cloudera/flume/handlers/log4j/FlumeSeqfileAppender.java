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
package com.cloudera.flume.handlers.log4j;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

import com.cloudera.flume.agent.MemoryMonitor;
import com.cloudera.flume.conf.Context;
import com.cloudera.flume.handlers.rolling.RollSink;

/**
 * This is a first stab at a reliable appender that will write to disk that will
 * get picked up by the agent and eventually sent to a collector. This should
 * allow for configuration options as well.
 * 
 * This is going to do file rolling based on a period of time or a certain size
 * of file (specified in the flume configuration) (currently, just a period of
 * time)
 * 
 * We cannot use the FlumeConfiguration because it causes problems with
 * initialization order
 * 
 * TODO (jon) Not sure if this works as a log4j appender anymore.
 */
public class FlumeSeqfileAppender extends AppenderSkeleton {
  RollSink appender;

  // options for users
  long checkmillis = 250;
  long maxage = 5000; // default to 5s
  String dir = "/var/log/flume";

  public FlumeSeqfileAppender() {
    MemoryMonitor.setupHardExitMemMonitor(.95);
  }

  public void activateOptions() {
    appender = new RollSink(new Context(), "dfs(\"file://" + dir
        + File.pathSeparator + "%{rolltag}\")", maxage, checkmillis);
  }

  protected void append(LoggingEvent e) {
    try {
      appender.append(new Log4JEventAdaptor(e));
    } catch (IOException e1) {
      e1.printStackTrace();
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
  }

  @Override
  public void close() {
    try {
      appender.close();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e1) {
      e1.printStackTrace();
    }
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }

  // //
  // Log4j Config options (as beans)

  public void setMaxAge(long age) {
    this.maxage = age;
  }

  public long getMaxAge() {
    return maxage;
  }

  public void setCheckMillis(long ms) {
    this.checkmillis = ms;
  }

  public long getCheckMillis() {
    return checkmillis;
  }

  public void setLogDir(String dir) {
    this.dir = dir;
  }

  public String getLogDir() {
    return dir;
  }

}
