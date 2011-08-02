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

import java.io.IOException;

import org.junit.Test;

import com.cloudera.flume.conf.FlumeSpecException;
import com.cloudera.flume.conf.thrift.FlumeConfigData;

/**
 * This test case implements a simple sink translator that adds a pass through
 * null decorator to the specified sink, and verifies that the expected
 * translations occur.
 */
public class TestTranslatingConfigurationManager {

  public String DEFAULTFLOW = "default-flow";

  /**
   * This does a simple translation to sinks.
   */
  class NullDecoTranslator extends TranslatingConfigurationManager {

    public NullDecoTranslator(ConfigurationManager parent,
        ConfigurationManager self) {
      super(parent, self);
    }

    @Override
    public String getName() {
      return "NullTranslator";
    }

    @Override
    public String translateSink(String logicalnode, String sink)
        throws FlumeSpecException {
      return "{ nullDeco => " + sink + " }";
    }

    @Override
    public String translateSource(String logicalnode, String source)
        throws FlumeSpecException {
      return source;
    }

  }

  /**
   * Sets a configuration and makes sure the state of the different managers are
   * the expected values.
   **/
  @Test
  public void testSetConf() throws IOException, FlumeSpecException {
    ConfigurationManager parent = new ConfigManager();
    ConfigurationManager self = new ConfigManager();
    ConfigurationManager trans = new NullDecoTranslator(parent, self);

    trans.setConfig("foo", DEFAULTFLOW, "null", "null");

    FlumeConfigData transData = trans.getConfig("foo");
    assertEquals("null", transData.getSourceConfig());
    assertEquals("{ nullDeco => null }", transData.getSinkConfig());

    FlumeConfigData selfData = self.getConfig("foo");
    assertEquals("null", selfData.getSourceConfig());
    assertEquals("{ nullDeco => null }", selfData.getSinkConfig());

    FlumeConfigData origData = parent.getConfig("foo");
    assertEquals("null", origData.getSourceConfig());
    assertEquals("null", origData.getSinkConfig());
  }

  /**
   * Sets a configuration and then does a refreshAll. This should remain the
   * same.
   */
  @Test
  public void testRefreshAll() throws IOException, FlumeSpecException {

    ConfigurationManager parent = new ConfigManager();
    ConfigurationManager self = new ConfigManager();
    ConfigurationManager trans = new NullDecoTranslator(parent, self);

    trans.setConfig("foo", DEFAULTFLOW, "null", "null");

    trans.refreshAll();

    FlumeConfigData transData = trans.getConfig("foo");
    assertEquals("null", transData.getSourceConfig());
    assertEquals("{ nullDeco => null }", transData.getSinkConfig());

    FlumeConfigData selfData = self.getConfig("foo");
    assertEquals("null", selfData.getSourceConfig());
    assertEquals("{ nullDeco => null }", selfData.getSinkConfig());

    FlumeConfigData origData = parent.getConfig("foo");
    assertEquals("null", origData.getSourceConfig());
    assertEquals("null", origData.getSinkConfig());
  }

  /**
   * Sets a configuration and then does a refreshAll. This should remain the
   * same.
   */
  @Test
  public void testUpdateAll() throws IOException, FlumeSpecException {

    ConfigurationManager parent = new ConfigManager();
    ConfigurationManager self = new ConfigManager();
    ConfigurationManager trans = new NullDecoTranslator(parent, self);

    trans.setConfig("foo", DEFAULTFLOW, "null", "null");

    trans.updateAll();

    FlumeConfigData transData = trans.getConfig("foo");
    assertEquals("null", transData.getSourceConfig());
    assertEquals("{ nullDeco => null }", transData.getSinkConfig());

    FlumeConfigData selfData = self.getConfig("foo");
    assertEquals("null", selfData.getSourceConfig());
    assertEquals("{ nullDeco => null }", selfData.getSinkConfig());

    FlumeConfigData origData = parent.getConfig("foo");
    assertEquals("null", origData.getSourceConfig());
    assertEquals("null", origData.getSinkConfig());
  }

}
