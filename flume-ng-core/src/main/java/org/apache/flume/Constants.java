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
package org.apache.flume;

public final class Constants {

  /**
   * Disables the fail-fast startup behavior. This would be used in the
   * scenario where the agent is expected to start, but the config
   * file be populated at a later point in time.
   */
  public static final String SYSPROP_CALLED_FROM_SERVICE
          = "flume.called.from.service";

  private Constants() {
    // disable explicit object creation
  }
}
