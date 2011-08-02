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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.util.FileUtil;

/**
 * Verify translation of a number of sources/sink combinations
 */
@RunWith(Parameterized.class)
public class TestFlumeConfigTranslation {
  public static Logger LOG = Logger.getLogger(TestFlumeConfigTranslation.class);

  private FlumeMaster fm;

  private File tmpdir = null;
  private FlumeConfiguration cfg = FlumeConfiguration
      .createTestableConfiguration();
  private CmdList cmds;

  public TestFlumeConfigTranslation(CmdList cmds) {
    this.cmds = cmds;
  }

  private static class Cmd {
    public Cmd(String name, String src, String snk) {
      this.name = name;
      this.src = src;
      this.snk = snk;
    }

    String name;
    String src;
    String snk;

    public String[] asArray() {
      return new String[] { name, src, snk };
    }
  }

  private static class CmdList extends ArrayList<Cmd> {
    private static final long serialVersionUID = 1L;
  }

  @Parameters
  public static Collection<Object[]> configs() {
    ArrayList<Object[]> data = new ArrayList<Object[]>();

    CmdList cl;

    cl = new CmdList();
    cl.add(new Cmd("node", "null", "null"));
    data.add(new Object[] { cl });

    cl = new CmdList();
    cl.add(new Cmd("A", "null", "agentSink(\"localhost\",73737)"));
    cl.add(new Cmd("B", "collectorSource(73737)", "null"));
    data.add(new Object[] { cl });

    cl = new CmdList();
    cl.add(new Cmd("A", "null", "agentDFOSink(\"localhost\",73737)"));
    cl.add(new Cmd("B", "collectorSource(73737)", "null"));
    data.add(new Object[] { cl });

    cl = new CmdList();
    cl.add(new Cmd("A", "null", "agentE2ESink(\"localhost\",73737)"));
    cl.add(new Cmd("B", "collectorSource(73737)", "null"));
    data.add(new Object[] { cl });

    cl = new CmdList();
    cl.add(new Cmd("A", "null", "agentBESink(\"localhost\",73737)"));
    cl.add(new Cmd("B", "collectorSource(73737)", "null"));
    data.add(new Object[] { cl });

    cl = new CmdList();
    cl.add(new Cmd("A", "null", "agentDFOChain(\"localhost\",73737)"));
    cl.add(new Cmd("B", "collectorSource(73737)", "null"));
    data.add(new Object[] { cl });

    cl = new CmdList();
    cl.add(new Cmd("A", "null", "agentE2EChain(\"localhost\",73737)"));
    cl.add(new Cmd("B", "collectorSource(73737)", "null"));
    data.add(new Object[] { cl });

    cl = new CmdList();
    cl.add(new Cmd("A", "null", "agentBEChain(\"localhost\",73737)"));
    cl.add(new Cmd("B", "collectorSource(73737)", "null"));
    data.add(new Object[] { cl });

    cl = new CmdList();
    cl.add(new Cmd("A", "null", "autoDFOChain"));
    cl.add(new Cmd("B", "autoCollectorSource", "null"));
    data.add(new Object[] { cl });

    cl = new CmdList();
    cl.add(new Cmd("A", "null", "autoE2EChain"));
    cl.add(new Cmd("B", "autoCollectorSource", "null"));
    data.add(new Object[] { cl });

    cl = new CmdList();
    cl.add(new Cmd("A", "null", "autoBEChain"));
    cl.add(new Cmd("B", "autoCollectorSource", "null"));
    data.add(new Object[] { cl });

    return data;
  }

  /**
   * Use the same master for all these tests - faster
   * 
   * @throws IOException
   */
  @Before
  public void setConfiguration() throws IOException {
    // Set directory of webapps to build-specific dir
    cfg.set(FlumeConfiguration.WEBAPPS_PATH, "build/webapps");
    // Give ZK a temporary directory, otherwise it's possible we'll reload some
    // old configs
    tmpdir = FileUtil.mktempdir();
    cfg.set(FlumeConfiguration.MASTER_ZK_LOGDIR, tmpdir.getAbsolutePath());

    Logger.getRootLogger().setLevel(Level.DEBUG);

    // would rather do this once for the whole test but unmapAll is unreliable
    // in some cases after the test failed unmapAll would fail and cause all
    // subsequent tests to fail
    fm = new FlumeMaster(cfg, false);
    fm.serve();
  }

  @After
  public void deleteConfigDir() throws IOException {
    fm.shutdown();

    if (tmpdir != null) {
      FileUtil.rmr(tmpdir);
      tmpdir = null;
    }
  }

  /**
   * Verify translation.
   */
  @Test
  public void testTranslateConfigString() throws Exception {
    Execable cfg = ConfigCommand.buildExecable();

    for (Cmd cmd : cmds) {
      cfg.exec(cmd.asArray());
    }

    assertEquals(cmds.size(), fm.getSpecMan().getAllConfigs().size());
    assertEquals(cmds.size(), fm.getSpecMan().getTranslatedConfigs().size());
  }
}
