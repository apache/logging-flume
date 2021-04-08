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
package org.apache.flume.register.service;

import org.apache.flume.Context;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.conf.Configurable;

@InterfaceAudience.Public
@InterfaceStability.Stable
public abstract class AbstractRegisterService implements RegisterService, Configurable {
  private String name;

  public AbstractRegisterService() { }

  @Override
  public synchronized void setName(String name) {
    this.name = name;
  }

  @Override
  public synchronized void start() throws Exception { }

  @Override
  public synchronized void stop() { }

  @Override
  public synchronized String getName() {
    return name;
  }

  @Override
  public synchronized void configure(Context context) { }

  public String toString() {
    return this.getClass().getName() + "{name: " + name + "}";
  }
}
