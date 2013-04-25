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
package org.apache.flume.source;


public class ExecSourceConfigurationConstants {

  /**
   * Should the exec'ed command restarted if it dies: : default false
   */
  public static final String CONFIG_RESTART = "restart";
  public static final boolean DEFAULT_RESTART = false;

  /**
   * Amount of time to wait before attempting a restart: : default 10000 ms
   */
  public static final String CONFIG_RESTART_THROTTLE = "restartThrottle";
  public static final long DEFAULT_RESTART_THROTTLE = 10000L;

  /**
   * Should stderr from the command be logged: default false
   */
  public static final String CONFIG_LOG_STDERR = "logStdErr";
  public static final boolean DEFAULT_LOG_STDERR = false;

  /**
   * Number of lines to read at a time
   */
  public static final String CONFIG_BATCH_SIZE = "batchSize";
  public static final int DEFAULT_BATCH_SIZE = 20;

  /**
   * Amount of time to wait, if the buffer size was not reached, before 
   * to data is pushed downstream: : default 3000 ms
   */
  public static final String CONFIG_BATCH_TIME_OUT = "batchTimeout";
  public static final long DEFAULT_BATCH_TIME_OUT = 3000l;

  /**
   * Charset for reading input
   */
  public static final String CHARSET = "charset";
  public static final String DEFAULT_CHARSET = "UTF-8";

  /**
   * Optional shell/command processor used to run command
   */
  public static final String CONFIG_SHELL = "shell";
}
