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

import java.util.HashMap;
import java.util.Map;

/**
 * This context is an abstraction for scopes. Prior to this there were one
 * scopes the physical node (essentially conf file). This allows for new
 * subscopes such as logical node contexts. This allows information to be passed
 * to specific logical nodes (such as logicalnode name).
 * 
 * This interface will likely subsume FlumeConfiguration (physical node scope),
 * and configuration information grabbed from the master such as as logical node
 * specific failover chains (master scope). With scope chaining and shadowing we
 * can in the future introduce node specific configuration settings
 * transparently.
 */
public class Context {
  Map<String, String> table = new HashMap<String, String>();
  final Context parent;

  final public static Context EMPTY = new Context(null);

  public Context(Context parent) {
    this.parent = parent;
  }

  public Context() {
    this.parent = EMPTY;
  }

  /**
   * Get the parent context
   */
  public Context getParent() {
    return null;
  }

  /**
   * Get a value from the current node, otherwise go up to a larger scope and
   * try to get the value from there.
   */
  public String getValue(String arg) {
    String val = table.get(arg);
    if (val == null && getParent() != null) {
      return getParent().getValue(arg);
    }
    return val;
  }

  protected void putValue(String arg, String val) {
    table.put(arg, val);
  }
}
