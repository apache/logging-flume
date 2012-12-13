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
package org.apache.flume.source;

import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.PollableSource;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;

/**
 * Base class which ensures sub-classes will inherit all the properties
 * of BasicSourceSemantics in addition to:
 * <ol>
 * <li>Ensuring when configure/start throw an exception process will not
 * be called</li>
 * <li>Ensure that process will not be called unless configure and start
 * have successfully been called</li>
 * </ol>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AbstractPollableSource extends BasicSourceSemantics
  implements PollableSource {

  public AbstractPollableSource() {
    super();
  }
  public Status process() throws EventDeliveryException {
    Exception exception = getStartException();
    if (exception != null) {
      throw new FlumeException("Source had error configuring or starting",
          exception);
    }
    if(!isStarted()) {
      throw new EventDeliveryException("Source is not started");
    }
    return doProcess();
  }

  protected abstract Status doProcess() throws EventDeliveryException;
}
