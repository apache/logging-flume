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
package com.cloudera.flume.reporter.histogram;

import com.cloudera.util.Histogram;

/**
 * Generic class for histogram chart generators. This generate method creates a
 * string that formats histogram.
 * 
 * This assumes that the histogram is fed every so often. Any axis, title,
 * legend information should be specified in the generator's constructor.
 * 
 * Type T is the type of the object used to bin values into histograms.
 */
abstract public class HistogramChartGen<T> {
  abstract public String generate(Histogram<T> h);
}
