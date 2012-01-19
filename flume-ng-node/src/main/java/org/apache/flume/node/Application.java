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

package org.apache.flume.node;

import java.io.File;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flume.ChannelFactory;
import org.apache.flume.Constants;
import org.apache.flume.SinkFactory;
import org.apache.flume.SourceFactory;
import org.apache.flume.channel.DefaultChannelFactory;
import org.apache.flume.conf.file.AbstractFileConfigurationProvider;
import org.apache.flume.conf.properties.PropertiesFileConfigurationProvider;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.node.nodemanager.DefaultLogicalNodeManager;
import org.apache.flume.sink.DefaultSinkFactory;
import org.apache.flume.source.DefaultSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class Application {

  private static final Logger logger = LoggerFactory
      .getLogger(Application.class);

  private String[] args;
  private File configurationFile;
  private String nodeName;

  private SourceFactory sourceFactory;
  private SinkFactory sinkFactory;
  private ChannelFactory channelFactory;

  public static void main(String[] args) {
    Application application = new Application();

    application.setArgs(args);

    try {
      if (application.parseOptions()) {
        application.run();
      }
    } catch (ParseException e) {
      logger.error(e.getMessage());
    } catch (Exception e) {
      logger.error("A fatal error occurred while running. Exception follows.",
          e);
    }
  }

  public Application() {
    sourceFactory = new DefaultSourceFactory();
    sinkFactory = new DefaultSinkFactory();
    channelFactory = new DefaultChannelFactory();
  }

  public boolean parseOptions() throws ParseException {
    Options options = new Options();

    Option option = new Option("n", "name", true, "the name of this node");
    options.addOption(option);

    option = new Option("f", "conf-file", true, "specify a conf file");
    options.addOption(option);

    option = new Option("h", "help", false, "display help text");
    options.addOption(option);

    CommandLineParser parser = new GnuParser();
    CommandLine commandLine = parser.parse(options, args);

    if (commandLine.hasOption('f')) {
      configurationFile = new File(commandLine.getOptionValue('f'));

      if (!configurationFile.exists()) {
        // If command line invocation, then need to fail fast
        if (System.getProperty(Constants.SYSPROP_CALLED_FROM_SERVICE) == null) {
          String path = configurationFile.getPath();
          try {
            path = configurationFile.getCanonicalPath();
          } catch (IOException ex) {
            logger.error("Failed to read canonical path for file: " + path, ex);
          }
          throw new ParseException(
              "The specified configuration file does not exist: " + path);
        }
      }
    }

    if (commandLine.hasOption('n')) {
      nodeName = commandLine.getOptionValue('n');
    }

    if (commandLine.hasOption('h')) {
      new HelpFormatter().printHelp("flume-ng node", options, true);

      return false;
    }

    return true;
  }

  public void run() throws LifecycleException, InterruptedException,
      InstantiationException {

    final FlumeNode node = new FlumeNode();
    DefaultLogicalNodeManager nodeManager = new DefaultLogicalNodeManager();
    AbstractFileConfigurationProvider configurationProvider =
        new PropertiesFileConfigurationProvider();

    configurationProvider.setChannelFactory(channelFactory);
    configurationProvider.setSourceFactory(sourceFactory);
    configurationProvider.setSinkFactory(sinkFactory);

    configurationProvider.setNodeName(nodeName);
    configurationProvider.setConfigurationAware(nodeManager);
    configurationProvider.setFile(configurationFile);

    Preconditions.checkState(configurationFile != null,
        "Configuration file not specified");
    Preconditions.checkState(nodeName != null, "Node name not specified");

    node.setName(nodeName);
    node.setNodeManager(nodeManager);
    node.setConfigurationProvider(configurationProvider);

    Runtime.getRuntime().addShutdownHook(new Thread("node-shutdownHook") {

      @Override
      public void run() {
        node.stop();
      }

    });

    node.start();
    LifecycleController.waitForOneOf(node, LifecycleState.START_OR_ERROR);
    LifecycleController.waitForOneOf(node, LifecycleState.STOP_OR_ERROR);
  }

  public String[] getArgs() {
    return args;
  }

  public void setArgs(String[] args) {
    this.args = args;
  }

}
