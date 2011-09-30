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
package com.cloudera.flume.core.connector;

import java.io.IOException;

import org.junit.Test;
import static org.junit.Assert.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Clock;

/**
 * This tests the restart of source, sink in various exception cases.
 * Three scenarios are tested
 *   1) testHandleException
 *   source.next and sink.append periodically throw IOException and RuntimeException
 *   The driver is suppose to retry closing and reopening the source or sink
 *   The test verifies that the flow continues with these exceptions.
 *   
 *   2) testBailedExceptions
 *   Apart from next() and append() exceptions, the sink.open throws RuntimeException
 *   This should cause the driver to close the flow as it can't reopen the sink.
 *   
 *   3) testInterruptedException
 *   Apart from IOException, the source.next periodically throws InterruptedException
 *   This should cause the driver to close the flow as the InterruptedException is not retried.
 */
public class TestDirectDriverExp {
  Logger LOG = LoggerFactory.getLogger(TestDirectDriver.class);

  // test sink to throw IOExceptions periodically in append.
  // Optionally throw periodic RuntimeExceptions in open
  public class ExpSink extends EventSink.Base {
    boolean doRunTimeExp; // if true then throw RuntimeExceptions in open
    static final int sinkExp = 7;
    int count = 1;

    public ExpSink() {
      doRunTimeExp = false;
    }

    public ExpSink(boolean needRunTimeExp) {
      doRunTimeExp = needRunTimeExp;
    }

    @Override
    public void append(Event e) throws IOException {
      count++;
      if (count % sinkExp == 0) {
        LOG.info("raising exception at " + count);
        throw new IOException("*** sink exp in append ***");
      }
      LOG.info(e.toString());
    }

    @Override
    public void close() throws IOException {
      LOG.info("close");
    }

    @Override
    public void open() throws RuntimeException {
      if (doRunTimeExp && (count % (sinkExp*2) == 0)) {
        LOG.info("raising exception at " + count);
        throw new RuntimeException ("*** runtime exp in sink open ***");
      }
      LOG.info("open");
    }
  };

  // test source to throw IOExceptions periodically in next.
  // optionally throws periodic InterruptedException in next
  public class ExpSource extends EventSource.Base {
    static final int sourceExp = 17;
    static final int maxEvents = 36;
    boolean doInterrupExp;
    int count = 1;

    public ExpSource() {
      doInterrupExp = false;
    }

    public ExpSource(boolean needInterruptExp ) {
      doInterrupExp = needInterruptExp;
    }

  @Override
  public Event next() throws InterruptedException {
    count++;
    if (count > maxEvents)
      return null;
    else if (doInterrupExp && (count % (sourceExp*2) == 0)) {
      LOG.info("raising exception at " + count);
      throw new InterruptedException ("*** interrupt exp in source next ***");
    }
    else if (count % sourceExp == 0) {
      LOG.info("raising exception at " + count);
      throw new RuntimeException("*** source exp ***");
    }
    else
      return new EventImpl(" junk ".getBytes());
  }

  @Override
  public void close() throws InterruptedException {
    LOG.info("close");
  }

  @Override
  public void open() throws RuntimeException {
    LOG.info("open");
  }

};


  // source and sink throw exceptions. Ensure that the driver continues to retry
  @Test
  public void testHandleException() throws IOException,
      InterruptedException, RuntimeException {
    EventSink sink = new ExpSink();
    EventSource source = new ExpSource();

    DirectDriver driver = new DirectDriver(source, sink);
    driver.start();
    Clock.sleep(2000); // let the insistent open try a few times.
    driver.stop();
    LOG.info("Exception completed. Connection should exit successfully");
    // The driver should have recovered from source/sink exception
    assertNull("The driver should have recovered from source/sink exceptions", driver.getException());
  }


  // source and sink throw exceptions. Ensure that the driver continues to retry
  // sink re-open throws InterruptedException exception at some point. The driver should close
  // the flow when it sees an exception on retry.
  @Test
  public void testBailedExceptions() throws IOException,
  InterruptedException, RuntimeException{
    EventSink sink = new ExpSink(true); // sink will throw RuntimeException in close
    EventSource source = new ExpSource();

    DirectDriver driver = new DirectDriver(source, sink);
    driver.start();
    Clock.sleep(2000); // let the insistent open try a few times.
    driver.stop();
    LOG.info("testBailedExceptions completed. Connection should exit with runtime error");
    // The driver should have seen RuntimeException
    assertNotNull("The driver should have seen RuntimeException ", driver.getException());
  }


  // source and sink throw exceptions. Ensure that the driver continues to retry
  // source.next throws Interrupted exception. The driver should should not try to
  // reopen the source at that point and close the flow.
  @Test
  public void testInterruptedException() throws IOException,
  InterruptedException, RuntimeException {
    EventSink sink = new ExpSink(false);
    EventSource source = new ExpSource(true);

      DirectDriver driver = new DirectDriver(source, sink);
      driver.start();
      Clock.sleep(2000); // let the insistent open try a few times.
      driver.stop();
      LOG.info("testInterruptedException completed. Connection should exit with interrupt error");
      // The driver should have seen InterruptedException
      assertNotNull("The driver should have seen InterruptedException ", driver.getException());
  }

}
