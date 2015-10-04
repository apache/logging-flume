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

public class PollableSourceConstants {

  public static final String BACKOFF_SLEEP_INCREMENT = "backoffSleepIncrement";
  public static final String MAX_BACKOFF_SLEEP = "maxBackoffSleep";
  public static final long DEFAULT_BACKOFF_SLEEP_INCREMENT = 1000;
  public static final long DEFAULT_MAX_BACKOFF_SLEEP = 5000;
}