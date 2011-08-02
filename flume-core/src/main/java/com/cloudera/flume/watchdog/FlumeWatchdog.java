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
package com.cloudera.flume.watchdog;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;

/**
 * This the program invoked for flume's watchdog. Basically invoke this program
 * with extra arguments that are the command to execute.
 * 
 * It may seem odd that this just has main. It was decided that it would be good
 * to have Flume in the command name. Except for one configuration file option,
 * the actual watchdog code is not dependent on flume, so it lives as a separate
 * class.
 */
public class FlumeWatchdog {

  static final Logger LOG = LoggerFactory.getLogger(FlumeWatchdog.class);

  public static void main(String[] argv) {
    if (argv.length == 0) {
      System.out.println("need to specify watched command as arguments");
      System.exit(-1);
    }

    // drop a pid file if these options are set.
    String pid = System.getProperty("pid");
    String pidfile = System.getProperty("pidfile");
    if (pidfile != null && pid != null) {
      File f = new File(pidfile);
      f.deleteOnExit();

      try {
        FileWriter fw = new FileWriter(f);
        fw.write(pid);
        fw.close();
      } catch (IOException e) {
        LOG.error("failed to drop a pid file", e);
        System.exit(-1);
      }
      LOG.info("Dropped a pidfile='" + pidfile + "' with pid=" + pid);
    } else {
      LOG.warn("No pid or pidfile system property specified.");
    }

    String interactiveprop = System.getProperty("fwdstdin");
    boolean interactive = (interactiveprop != null);

    String[] args = argv;

    FlumeConfiguration conf = FlumeConfiguration.hardExitLoadConfig();
    int maxTriesPerMin = conf.getMaxRestartsPerMin();

    Watchdog watchdog = new Watchdog(args, interactive);
    watchdog.run(maxTriesPerMin);
  }
}
