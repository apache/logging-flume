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
import org.apache.flume.NamedComponent;

/**
 * Interface to register service. Implementing service should register 
 * service and start and un-register on stop
 */
public interface RegisterService extends NamedComponent {

  public abstract void start() throws Exception;
  
  public abstract void stop();

  public abstract void configure(Context context);
}
