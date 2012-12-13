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
import static org.mockito.Mockito.*;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.junit.Before;
import org.junit.Test;

public class TestAbstractPollableSource {

  private AbstractPollableSource source;

  @Before
  public void setUp() {
    source = spy(new AbstractPollableSource() {
      @Override
      protected Status doProcess() throws EventDeliveryException {
        return Status.BACKOFF;
      }
      @Override
      protected void doConfigure(Context context) throws FlumeException {
        throw new FlumeException("dummy");
      }
      @Override
      protected void doStart() throws FlumeException {

      }
      @Override
      protected void doStop() throws FlumeException {

      }
    });
  }

  @Test(expected = FlumeException.class)
  public void testExceptionStartup() throws Exception {
    source.configure(new Context());
  }
  @Test(expected = EventDeliveryException.class)
  public void testNotStarted() throws Exception {
    source.process();
  }

}
