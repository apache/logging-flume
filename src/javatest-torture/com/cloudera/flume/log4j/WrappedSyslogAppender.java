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
package com.cloudera.flume.log4j;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.net.SyslogAppender;
import org.apache.log4j.spi.LoggingEvent;

/**
 * This was my first hook into log4j -- a simple wrapper around the
 * SyslogAppender.
 * 
 * syslog fails gracefully by dropping packets, but has terrible goodput (2-3
 * MB/s, 20-25k events per second).
 */
public class WrappedSyslogAppender extends AppenderSkeleton {
  SyslogAppender syslog;

  public WrappedSyslogAppender() {
    syslog = new SyslogAppender();
    syslog.setFacility("LOCAL1");
    syslog.setSyslogHost("localhost");
  }

  @Override
  protected void append(LoggingEvent event) {
    syslog.append(event);
  }

  public void close() {
    syslog.close();
  }

  public boolean requiresLayout() {
    return syslog.requiresLayout();
  }

  public void setLayout(Layout layout) {
    syslog.setLayout(layout);
  }

  public Layout getLayout() {
    return syslog.getLayout();
  }

}
