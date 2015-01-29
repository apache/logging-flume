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

package org.apache.flume.sink.kite.policy;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A failure policy that logs the error and then forces a retry by throwing
 * {@link EventDeliveryException}.
 */
public class RetryPolicy implements FailurePolicy {
  private static final Logger LOG = LoggerFactory.getLogger(RetryPolicy.class);

  private RetryPolicy() {
  }

  @Override
  public void handle(Event event, Throwable cause) throws EventDeliveryException {
    LOG.error("Event delivery failed: " + cause.getLocalizedMessage());
    LOG.debug("Exception follows.", cause);

    throw new EventDeliveryException(cause);
  }

  @Override
  public void sync() throws EventDeliveryException {
    // do nothing
  }

  @Override
  public void close() throws EventDeliveryException {
    // do nothing
  }

  public static class Builder implements FailurePolicy.Builder {

    @Override
    public FailurePolicy build(Context config) {
      return new RetryPolicy();
    }

  }
}
