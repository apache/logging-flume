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
package org.apache.flume;

import java.util.List;

import org.apache.flume.Sink.Status;
import org.apache.flume.conf.Configurable;
import org.apache.flume.lifecycle.LifecycleAware;

/**
 * Interface for a device that allows abstraction of the behavior of multiple
 * sinks, always assigned to a SinkRunner
 */
public interface SinkProcessor extends LifecycleAware, Configurable {
  /**
   * Handle a request to poll the owned sinks.
   * 
   * The processor is expected to call process on whatever sink(s) appropriate,
   * handling failures as appropriate and throwing EventDeliveryException in
   * unrecoverable situations.
   * 
   * @return OK or TRY_LATER
   * @throws EventDeliveryException
   *           in case of unrecoverable problems
   */
  Status process() throws EventDeliveryException;

  /**
   * Set all sinks to work with.
   * 
   * Sink specific parameters are passed to the processor via configure
   * 
   * @param sinks
   */
  void setSinks(List<Sink> sinks);
}