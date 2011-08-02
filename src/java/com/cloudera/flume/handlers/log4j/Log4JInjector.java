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
package com.cloudera.flume.handlers.log4j;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * This test program generates a specified number of logs using a specified
 * log4j properties configuration file. This is useful for testing properties
 * files and for doing performance tests.
 * 
 */
public class Log4JInjector {
  public static void usage() {
    System.out
        .println("need to specify <log4jprop file> <# of messages> message");
  }

  public static void main(String argv[]) {
    // String curDir = System.getProperty("user.dir");
    // System.out.println("pwd == " + curDir);

    if (argv.length < 2) {
      usage();
      System.exit(-1);
    }

    boolean endless = false;
    boolean randsleep = false;
    long sleep = 0;
    String props = null;
    long iters = 100;
    StringBuffer b = new StringBuffer();
    for (int j = 0; j < argv.length; j++) {

      // handle options
      if ("-endless".equals(argv[j])) {
        endless = true;
        continue;
      }

      if ("-sleep".equals(argv[j])) {
        j++;
        sleep = Long.parseLong(argv[j]);
        continue;
      }

      if ("-randsleep".equals(argv[j])) {
        randsleep = true;
        continue;
      }

      // handle the rest.
      props = argv[j];
      j++;
      iters = Long.parseLong(argv[j]);
      j++;

      boolean first = true;
      for (int i = j; i < argv.length; i++) {
        if (!first) {
          b.append(" ");
        }

        b.append(argv[i]);
        first = false;
      }
      break;
    }
    PropertyConfigurator.configure(props);
    Logger logger = Logger.getLogger(Log4JInjector.class.getName());

    // send test for specified number of iterations.
    for (long l = 0; l < iters || endless; l++) {
      try {
        long thisSleep = (long) ((double) sleep * (double) (randsleep ? Math
            .random() : 1));
        Thread.sleep(thisSleep);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      logger.warn("" + l + " " + b);
    }
  }
}
