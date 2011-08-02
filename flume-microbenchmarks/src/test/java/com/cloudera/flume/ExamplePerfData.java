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
package com.cloudera.flume;

/**
 * This contains pointers to files use for performance testing. These files are
 * large (~100MB each) and are not checked into the repository.
 */
public interface ExamplePerfData extends ExampleData {
  final static String HADOOP_DATA[] = { "src/javaperf/data/hadoop_00000",
  // "src/javaperf/data/hadoop_00001",
  // TODO (jon) this goes to 53, only 00000 is present in data dir.
  };

}
