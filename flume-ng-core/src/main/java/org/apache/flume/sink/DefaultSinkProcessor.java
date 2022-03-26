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
package org.apache.flume.sink;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Sink.Status;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.conf.ConfigurableComponent;

/**
 * Default sink processor that only accepts a single sink, passing on process
 * results without any additional handling. Suitable for all sinks that aren't
 * assigned to a group.
 */
public class DefaultSinkProcessor extends AbstractSingleSinkProcessor implements ConfigurableComponent {

  @Override
  public void configure(Context context) {
  }

  @Override
  public Status process() throws EventDeliveryException {
    return sink.process();
  }

  @Override
  public void configure(ComponentConfiguration conf) {

  }

}
