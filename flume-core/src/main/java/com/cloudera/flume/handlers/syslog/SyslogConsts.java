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
package com.cloudera.flume.handlers.syslog;

import java.util.regex.Pattern;

import com.cloudera.flume.core.Event.Priority;

/**
 * constants and lookup tables for syslog constants live here.
 * 
 * TODO(henry): This should not be an interface!
 */
public interface SyslogConsts {
  final public static String SYSLOG_FACILITY = "syslogfacility";
  final public static String SYSLOG_SEVERITY = "syslogseverity";

  final public static String[] FACILITY = { "kernel", "user", "mail", "system",
      "sec/auth", // 0-4
      "syslog", "lpr", "news", "uucp", "clock", // 5-9
      "sec/auth", "ftp", "ntp", "log audit", "log alert", // 10-14
      "clock", "local0", "local1", "local2", "local3", // 15-19
      "local4", "local5", "local6", "local7" // 20-23
  };

  /**
   * A mapping from syslog priorities to the priority levels in flume.
   */
  final static Priority[] SEVERITY = { Priority.FATAL, // 0 emergency
      Priority.FATAL, // 1 alert
      Priority.FATAL, // 2 critical
      Priority.ERROR, // 3 error
      Priority.WARN, // 4 warning
      Priority.INFO, // 5 notice
      Priority.INFO, // 6 info
      Priority.DEBUG // 7 debug
  };

  /**
   * This is a backup translation in case the fields are not present.
   */
  final static int[] PRIO2SEVERITY = { 0, // flume.FATAL -> SL.FATAL
      3, // flume.ERROR -> SL.ERROR
      4, // flume.WARNING -> SL.WARNING
      5, // flume.INFO -> SL.NOTICE
      7, 7 // flume.DEBUG, flume.TRACE -> SL.DEBUG
  };

  // pattern for extracting info out of a raw over the wire syslog event.
  final static Pattern SYSLOG_WIRE_PATTERN = Pattern
      .compile("<(\\d{1,3})>(.*)");

}
