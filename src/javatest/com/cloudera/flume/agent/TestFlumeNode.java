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

package com.cloudera.flume.agent;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeBuilder;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory;
import com.cloudera.flume.conf.thrift.FlumeConfigData;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.debug.NullSink;
import com.cloudera.flume.handlers.syslog.SyslogTcpSourceThreads;
import com.cloudera.flume.master.CommandManager;
import com.cloudera.flume.master.ConfigManager;
import com.cloudera.flume.master.ConfigStore;
import com.cloudera.flume.master.FlumeMaster;
import com.cloudera.flume.master.MasterAckManager;
import com.cloudera.flume.master.StatusManager;
import com.cloudera.util.Clock;

/**
 * This tests error/exception handling mechanisms in on LogicalNode
 */
public class TestFlumeNode extends TestCase {

  public static Logger LOG = Logger.getLogger(TestFlumeNode.class);

  /**
   * There was a bug -- when source that threw exception when closing twice
   * (should be ok) that prevented progress.
   * 
   * In this, an exception is thrown on any close this would have forced the
   * problem and shows that progress can be made. loadNode only throws if open
   * fails.
   */

  public void testSurviveCloseException() throws IOException {
    LogicalNode node = new LogicalNode(new Context(), "test");

    class CloseExnSource extends EventSource.Base {
      @Override
      public void close() throws IOException {
        throw new IOException("arg!");
      }

      @Override
      public Event next() throws IOException {
        return null;
      }

      @Override
      public void open() throws IOException {
      }
    }
    ;

    node.openLoadNode(new CloseExnSource(), new NullSink());
    node.openLoadNode(new CloseExnSource(), new NullSink());
  }

  public void testRestartNextException() throws Exception {
    LogicalNode node = new LogicalNode(new Context(), "test");

    final AtomicInteger count = new AtomicInteger();

    class NextExnSource extends EventSource.Base {
      @Override
      public void close() throws IOException {
        System.out.println("Next Exn Source closed");
      }

      @Override
      public Event next() throws IOException {
        int i = count.incrementAndGet();
        if (i > 100) {
          return null; // close naturally
        }

        // "fail"
        throw new IOException("ayiii!");
      }

      @Override
      public void open() throws IOException {
        System.out.println("Next Exn Source opened");
      }
    }
    ;

    FlumeBuilder.setSourceFactory(new SourceFactory() {
      public EventSource getSource(String name, String... args) {
        return new NextExnSource();
      }
    });

    // 0 is the first cfg version
    FlumeConfigData cfg = new FlumeConfigData(0, "null", "null", 0, 0,
        "my-test-flow");
    node.loadConfig(cfg); // this will load the NextExnSource and a NullSink

    // sleep so that we open-append-fail-close, open-append-fail-close
    // multiple times.
    Clock.sleep(5000); // TODO (jon) replace with countdownlatch

    System.out.printf("next called %d times", count.get());
    System.out.flush();
    // node.close(); // this is a differetn testnow

    // successful if open-append-close has been done many times.
    assertTrue(count.get() > 1);
  }

  public void testFailfastOutException() throws IOException {
    LogicalNode node = new LogicalNode(new Context(), "test");

    class OpenExnSource extends EventSource.Base {
      @Override
      public void close() throws IOException {
      }

      @Override
      public Event next() throws IOException {
        return null;
      }

      @Override
      public void open() throws IOException {
        throw new IOException("bork!");
      }
    }
    ;

    // there should be no exception thrown in this thread. (errors are thrown by
    // logical node and handled at that level)
    node.openLoadNode(new OpenExnSource(), new NullSink());
  }

  /**
   * This tests to make sure that openLoadNode opens newly specified sources,
   * and closes previous sources when a new one is specified.
   */
  public void testOpenCloseOpenIsOpen() throws IOException {
    class IsOpenSource extends EventSource.Base {
      boolean isOpen = false;

      @Override
      public void close() throws IOException {
        isOpen = false;
      }

      @Override
      public Event next() throws IOException {
        Event e = new EventImpl(new byte[0]); // / do not return null.
        updateEventProcessingStats(e);
        return e;
      }

      @Override
      public void open() throws IOException {
        isOpen = true;
      }
    }
    ;

    LogicalNode node = new LogicalNode(new Context(), "test");
    IsOpenSource prev = new IsOpenSource();
    node.openLoadNode(prev, new NullSink());
    node.getSource().next(); // force lazy source to open
    for (int i = 0; i < 10; i++) {

      assertTrue(prev.isOpen);
      IsOpenSource cur = new IsOpenSource();
      node.openLoadNode(cur, new NullSink());
      node.getSource().next(); // force lazy source to open.
      assertFalse(prev.isOpen);
      assertTrue(cur.isOpen);
      prev = cur;
    }
  }

  /**
   * This test makes sure there the openLoadNode behaviour works where the new
   * source is opened and the old sink is closed. (and no resource contention
   * IOExceptions are triggered.
   */
  public void testOpenCloseSyslogTcpSourceThreads() throws IOException {
    LogicalNode node = new LogicalNode(new Context(), "test");
    EventSource prev = new SyslogTcpSourceThreads(6789);
    node.openLoadNode(prev, new NullSink());
    for (int i = 0; i < 20; i++) {
      EventSource cur = new SyslogTcpSourceThreads(6789);
      node.openLoadNode(cur, new NullSink());
      prev = cur;

    }
  }

  /**
   * This test starts a node, then starts a master, and waits until a node shows
   * up. Then it closes, down and does so again, demonstrating that a node will
   * reconnect
   */
  public void testFlumeNodeReconnect() throws TTransportException, IOException,
      InterruptedException {

    FlumeConfiguration cfg = FlumeConfiguration.createTestableConfiguration();
    Clock.resetDefault();
    // Set directory of webapps to build-specific dir
    cfg.set(FlumeConfiguration.WEBAPPS_PATH, "build/webapps");
    // Doesn't matter whether or not we use ZK - use memory for speed
    cfg.set(FlumeConfiguration.MASTER_STORE, "memory");

    FlumeNode node = new FlumeNode(cfg);
    node.start();

    // avoiding gossip ack manager until it shuts down cleanly.
    ConfigStore cfgStore = FlumeMaster.createConfigStore(cfg);
    FlumeMaster fm = new FlumeMaster(new CommandManager(), new ConfigManager(
        cfgStore), new StatusManager(), new MasterAckManager(), cfg);

    assertEquals(0, fm.getKnownNodes().size());
    fm.serve();
    LOG.info("flume master 1 open ");
    while (fm.getKnownNodes().size() == 0) {
      Clock.sleep(1000);
    }
    fm.shutdown();
    LOG.info("flume master 1 closed");

    ConfigStore cfgStore2 = FlumeMaster.createConfigStore(cfg);
    FlumeMaster fm2 = new FlumeMaster(new CommandManager(), new ConfigManager(
        cfgStore2), new StatusManager(), new MasterAckManager(), cfg);
    assertEquals(0, fm2.getKnownNodes().size());
    fm2.serve();
    LOG.info("flume master 2 open ");
    while (fm2.getKnownNodes().size() == 0) {
      Clock.sleep(1000);
    }
    fm2.shutdown();
    LOG.info("flume master 2 closed");

  }
}
