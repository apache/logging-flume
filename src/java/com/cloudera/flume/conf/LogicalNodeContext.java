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
 * Information specific to a logical node context
 * 
 * This should be simple enough to generate at the logical node as a
 * configuration is being generated.
 */
public class LogicalNodeContext extends Context {

  public static final String C_LOGICAL = "logicalNodeName";
  public static final String C_PHYSICAL = "physicalNodeName";

  public LogicalNodeContext(String physName, String logicalName) {
    super();
    putValue(C_PHYSICAL, physName);
    putValue(C_LOGICAL, logicalName);
  }

  public LogicalNodeContext(Context parent, String physName, String logicalName) {
    super(parent);
    putValue(C_PHYSICAL, physName);
    putValue(C_LOGICAL, logicalName);
  }

}
