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

package com.cloudera.flume.conf;

/**
 * This context is used only to signal to Reportables that they should register
 * themselves with the report manager.
 */
public class ReportTestingContext extends Context {
  public static final String TESTING_REPORTS = "TESTING_REPORTS";
  public ReportTestingContext() {
    this.putValue(TESTING_REPORTS, "true");
  }
  
  public ReportTestingContext(Context context) {
    super(context);
    this.putValue(TESTING_REPORTS, "true");
  }
}
