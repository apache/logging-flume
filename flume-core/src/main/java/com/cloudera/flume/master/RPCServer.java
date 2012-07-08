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
package com.cloudera.flume.master;

import java.io.IOException;

/**
 * This interface is for specific RPC implementations of the server side 
 * of an RPC. Since the function signatures may be different depending on RPC 
 * package-specific data types, we only include start/stop methods here.  
 *
 */
public interface RPCServer {
  
  /**
   * Start listening for client requests.
   */
  public void serve() throws IOException;
  
  /**
   * Un-bind from the listen port.
   */
  public void stop() throws IOException;
}
