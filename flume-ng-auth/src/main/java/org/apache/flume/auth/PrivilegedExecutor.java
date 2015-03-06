/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.flume.auth;

import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;


/**
 * PrivilegedExecutor provides the ability to execute a PrivilegedAction
 * or a PrivilegedExceptionAction. Implementors of this class, can chose to execute
 * in normal mode or secure authenticated mode
 */
public interface PrivilegedExecutor {
  /**
   * This method is used to execute a privileged action, the implementor can
   * chose to execute the action using the appropriate privileges
   *
   * @param action A PrivilegedExceptionAction to perform as the desired user
   * @param <T> The return type of the action
   * @return T the T value returned by action.run()
   * @throws Exception
   */
  public <T> T execute(PrivilegedExceptionAction<T> action) throws Exception;

  /**
   * This method is used to execute a privileged action, the implementor can
   * chose to execute the action using the appropriate privileges
   *
   * @param action A PrivilegedAction to perform as the desired user
   * @param <T> The return type of the action
   * @return T the T value returned by action.run()
   */
  public <T> T execute(PrivilegedAction<T> action);
}


