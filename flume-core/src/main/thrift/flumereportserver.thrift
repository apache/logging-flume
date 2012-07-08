/*
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
# A simple Report server using Thrift
namespace java com.cloudera.flume.reporter.server.thrift

struct ThriftFlumeReport {
  3: map<string, string> stringMetrics,
  4: map<string, i64> longMetrics,
  5: map<string, double> doubleMetrics
}

service ThriftFlumeReportServer {
  // returns a serializable report with given name or null if report doesn't exist
  map<string, ThriftFlumeReport> getAllReports(),
  // returns a map of reports in serializable form
  ThriftFlumeReport getReportByName(1: string reportName),
}
