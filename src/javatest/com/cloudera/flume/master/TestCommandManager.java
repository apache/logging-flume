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

package com.cloudera.flume.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.util.Clock;

/**
 * The command manager needs to be able open and close with sane semantics.
 */
public class TestCommandManager {
  public static Logger LOG = Logger.getLogger(TestCommandManager.class);

  @Before
  public void setCfg() {
    // Isolate tests by only using simple cfg store
    FlumeConfiguration.get().set(FlumeConfiguration.MASTER_STORE, "memory");
  }

  /**
   * Tests to make sure stop is blocks until thread is essentially done and that
   * subsequent starts don't get into an odd state. If there should be no
   * exceptions thrown -- this indicates problems in the shutdown or startup.
   * 
   * There are no guarentees that all of the entries have executed.
   */
  public void testStartStop() {
    CommandManager cmdman = new CommandManager();

    for (int i = 0; i < 10; i++) {
      LOG.info("command manager start stop " + i);
      cmdman.start();
      Command cmd = new Command("noop");
      cmdman.submit(cmd);
      cmdman.stop();
    }
  }

  /**
   * We allow only one thread; if multiple starts then ignore second
   */
  @Test
  public void testStartStartStop() {
    CommandManager cmdman = new CommandManager();
    cmdman.start();
    cmdman.start();
    cmdman.stop();
  }

  /**
   * This submits 10 noop commands to the command manager.
   */
  @Test
  public void testCommandManager() throws InterruptedException {
    CommandManager cmdman = new CommandManager();

    List<Long> ids = new ArrayList<Long>();
    for (int i = 0; i < 10; i++) {
      LOG.info("command manager start stop " + i);
      Command cmd = new Command("noop");
      long id = cmdman.submit(cmd);
      ids.add(id);

      assertFalse(cmdman.isFailure(id));
      assertFalse(cmdman.isSuccess(id));
    }

    // all commands should be queued
    for (Long l : ids) {
      CommandStatus status = cmdman.getStatus(l);
      assertEquals("", status.getMessage());
      assertTrue(status.isQueued());
    }

    // this should be plenty of time to execute noops
    cmdman.start();
    Clock.sleep(200);
    cmdman.stop();

    // all commands should have executed successfully by now.
    for (Long l : ids) {
      CommandStatus status = cmdman.getStatus(l);
      assertEquals("", status.getMessage());
      assertTrue(status.isSuccess());

      assertFalse(cmdman.isFailure(l));
      assertTrue(cmdman.isSuccess(l));

    }

    LOG.info(cmdman.getReport());
  }

  /**
   * This is normally only used by the JSP forms to fill in values.
   */
  @Test
  public void testMultiConfigForm() {
    MultiConfigCommand mcc = new MultiConfigCommand();
    String spec = "node: null | null;";
    mcc.setSpecification(spec);
    Command c = mcc.toCommand();
    assertEquals("multiconfig", c.getCommand());
    assertEquals(spec, c.getArgs()[0]);
  }

  /**
   * JSP config form
   */
  @Test
  public void testConfigForm() {
    ConfigCommand cc = new ConfigCommand();
    cc.setNode("node");
    cc.setSource("src");
    cc.setSink("sink");

    Command c = cc.toCommand();
    assertEquals("config", c.getCommand());
    assertEquals("node", c.getArgs()[0]);
    assertEquals("src", c.getArgs()[1]);
    assertEquals("sink", c.getArgs()[2]);
  }

  @Test
  public void testConfigFormChoice() {
    ConfigCommand cc = new ConfigCommand();
    cc.setNodeChoice("chosen");
    cc.setSource("src");
    cc.setSink("sink");

    Command c = cc.toCommand();
    assertEquals("config", c.getCommand());
    assertEquals("chosen", c.getArgs()[0]);
    assertEquals("src", c.getArgs()[1]);
    assertEquals("sink", c.getArgs()[2]);
  }

  @Test
  public void testConfigFormChoiceLoses() {
    ConfigCommand cc = new ConfigCommand();
    cc.setNodeChoice("loser");
    cc.setNode("chosen");
    cc.setSource("src");
    cc.setSink("sink");

    Command c = cc.toCommand();
    assertEquals("config", c.getCommand());
    assertEquals("chosen", c.getArgs()[0]);
    assertEquals("src", c.getArgs()[1]);
    assertEquals("sink", c.getArgs()[2]);
  }

  @Test(expected = NullPointerException.class)
  public void testSubmitNull() throws InterruptedException {
    CommandManager cmdman = new CommandManager();
    cmdman.submit(null);
  }

  @Test(expected = NullPointerException.class)
  public void testExecNull() throws MasterExecException {
    CommandManager cmdman = new CommandManager();
    cmdman.exec(null);
  }

  @Test(expected = NullPointerException.class)
  public void testExecCmdNull() throws MasterExecException {
    CommandManager cmdman = new CommandManager();
    cmdman.exec(new Command(null));
  }

  @Test(expected = MasterExecException.class)
  public void testExecInvalidCmd() throws MasterExecException {
    CommandManager cmdman = new CommandManager();
    cmdman.exec(new Command("illegal"));
  }

  @Test(expected = MasterExecException.class)
  public void testExecIOE() throws MasterExecException {
    Execable ex = new Execable() {
      @Override
      public void exec(String[] args) throws MasterExecException, IOException {
        throw new IOException("");
      }
    };

    CommandManager cmdman = new CommandManager();
    cmdman.addCommand("exe", ex);
    cmdman.exec(new Command("exe"));
  }

  @Test(expected = MasterExecException.class)
  public void testExecIAE() throws MasterExecException {
    Execable ex = new Execable() {
      @Override
      public void exec(String[] args) throws MasterExecException, IOException {
        throw new IllegalArgumentException("");
      }
    };

    CommandManager cmdman = new CommandManager();
    cmdman.addCommand("exe", ex);
    cmdman.exec(new Command("exe"));
  }

  @Test
  public void testAddOverride() throws MasterExecException {
    final AtomicInteger i = new AtomicInteger(0);

    Execable ex = new Execable() {
      @Override
      public void exec(String[] args) throws MasterExecException, IOException {
        i.set(1);
      }
    };

    Execable ex2 = new Execable() {
      @Override
      public void exec(String[] args) throws MasterExecException, IOException {
        i.set(2);
      }
    };

    CommandManager cmdman = new CommandManager();
    cmdman.addCommand("exe", ex);
    cmdman.addCommand("exe", ex2);
    cmdman.exec(new Command("exe"));
    assertEquals(2, i.get());
  }

  /**
   * With no valid command in history, both should return false.
   */
  @Test
  public void testCheckInvalidID() {
    CommandManager cmdman = new CommandManager();
    assertFalse(cmdman.isFailure(0));
    assertFalse(cmdman.isSuccess(0));
  }

  @Test
  public void testExecFailure() throws MasterExecException,
      InterruptedException {
    Execable ex = new Execable() {
      @Override
      public void exec(String[] args) throws MasterExecException, IOException {
        throw new IllegalArgumentException("failure here");
      }
    };

    CommandManager cmdman = new CommandManager();
    cmdman.addCommand("exe", ex);
    long id = cmdman.submit(new Command("exe"));
    cmdman.start();
    Clock.sleep(100);
    cmdman.stop();
    assertTrue(cmdman.isFailure(id));
  }

  @Test
  public void testHandleCommand() {
    Execable ex = new Execable() {
      @Override
      public void exec(String[] args) throws MasterExecException, IOException {
        throw new IllegalArgumentException("failure here");
      }
    };

    CommandManager cmdman = new CommandManager();
    cmdman.addCommand("err", ex);

    cmdman.handleCommand(null); // do nothing.

    // submit a error command
    long id = cmdman.submit(new Command("err"));
    CommandStatus stat = cmdman.getStatus(id);
    cmdman.handleCommand(stat);
    assertTrue(cmdman.isFailure(id));

    id = cmdman.submit(new Command("noop"));
    stat = cmdman.getStatus(id);
    cmdman.handleCommand(stat);
    assertTrue(cmdman.isSuccess(id));

  }

  /**
   * Uninteresting test, just to get coverage.
   */
  public void testName() {
    CommandManager cmdman = new CommandManager();
    cmdman.getName();
  }

  @Test
  public void testSubmitConfigCommand() throws InterruptedException {
    CommandManager cmdman = new CommandManager();
    cmdman.start();
    // send valid commands with good and bad configs
    // Note - this calls into FlumeMaster.getInstance, which should normally
    // require it to be started. We set the cfgStore to "memory" so we don't
    // have to bother with serve and stop
    long good = cmdman.submit(new Command("config", "node", "null", "null"));
    long bad = cmdman.submit(new Command("config", "node", "fargle", "foogle"));
    Clock.sleep(1000);
    cmdman.stop();

    assertTrue(cmdman.isSuccess(good));
    assertTrue(cmdman.isFailure(bad));
  }

  @Test
  public void testSubmitMultiConfigCommand() throws InterruptedException {
    CommandManager cmdman = new CommandManager();
    cmdman.start();
    // send valid commands with good and bad configs
    long good = cmdman.submit(new Command("multiconfig",
        "node1 : null | null; node2: null | null;"));
    long badSyntax = cmdman.submit(new Command("multiconfig",
        "node3 : null | null; node4: null| null")); // forgot semi
    long badVals = cmdman.submit(new Command("multiconfig",
        "node5f : null | null; node6: foogle| bargle;"));

    // TODO (jon) add signal so that this command returns when command is
    // resolved to be error or success
    Clock.sleep(10000);
    cmdman.stop();

    assertTrue(cmdman.isSuccess(good));
    assertTrue(cmdman.isFailure(badSyntax));
    assertTrue(cmdman.isFailure(badVals));

    assertNotNull(FlumeMaster.getInstance().getSpecMan().getConfig("node1"));
    assertNotNull(FlumeMaster.getInstance().getSpecMan().getConfig("node2"));
    assertNull(FlumeMaster.getInstance().getSpecMan().getConfig("node3"));
    assertNull(FlumeMaster.getInstance().getSpecMan().getConfig("node4"));
    assertNull(FlumeMaster.getInstance().getSpecMan().getConfig("node5"));
    assertNull(FlumeMaster.getInstance().getSpecMan().getConfig("node6"));
  }

  @Test
  public void testUnmapAllCommand() throws InterruptedException {
    CommandManager cmdman = new CommandManager();
    cmdman.start();
    // send valid commands with good and bad configs
    long good = cmdman.submit(new Command("multiconfig",
        "node1 : null | null; node2: null | null;"));
    long logical1 = cmdman.submit(new Command("spawn", "physical", "node1"));
    long logical2 = cmdman.submit(new Command("spawn", "physical", "node2"));

    long unmap = cmdman.submit(new Command("unmapAll"));

    // TODO (jon) add signal so that this command returns when command is
    // resolved to be error or success
    Clock.sleep(10000);
    assertTrue(cmdman.isSuccess(good));
    assertTrue(cmdman.isSuccess(logical1));
    assertTrue(cmdman.isSuccess(logical2));
    assertTrue(cmdman.isSuccess(unmap));

    ConfigurationManager cfg = FlumeMaster.getInstance().getSpecMan();
    assertNotNull(cfg.getConfig("node1"));
    assertNotNull(cfg.getConfig("node2"));
    assertFalse(cfg.getLogicalNode("physical").contains("node1"));
    assertFalse(cfg.getLogicalNode("physical").contains("node2"));
  }

  @Test
  public void testNoopSleep() throws InterruptedException {
    CommandManager cmdman = new CommandManager();
    cmdman.start();

    long start = Clock.unixTime();
    long good = cmdman.submit(new Command("noop", "5000"));

    CommandStatus stat = null;
    do {
      Clock.sleep(100);
      stat = cmdman.getStatus(good);
    } while (stat.isInProgress());

    assertTrue(cmdman.isSuccess(good));
    assertTrue(Clock.unixTime() - start >= 5000);
  }
}
