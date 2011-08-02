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

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assume;
import org.junit.Test;

import com.cloudera.flume.ExampleData;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.util.OSUtils;

/*
 * This checks a few command line executions to make sure they terminate reasonably.
 * 
 * Also exercises startup checks to make sure we build out expectedly when there are the setup environment is insufficient.
 */
public class TestFlumeNodeMain {

  @Test
  public void testOneshot() throws InterruptedException {
    final String[] simple = {
        "-1",
        "-n",
        "test",
        "-c",
        "test: text(\""
            + getClass().getClassLoader()
                .getResource(ExampleData.APACHE_REGEXES).getFile()
            + "\") | null;" };
    final AtomicReference<Exception> ref = new AtomicReference<Exception>();
    Thread t = new Thread() {

      public void run() {
        try {
          FlumeNode.setup(simple);
        } catch (Exception e) {
          ref.set(e);
        }
      }
    };
    t.start();

    Thread.sleep(5000); // this should finish in less than 5 seconds
    if (ref.get() != null) {
      fail("an exception was thrown");
    }
  }

  @Test(expected = IOException.class)
  public void testAlreadyFile() throws IOException, InterruptedException {
    // set log dir to a place we control and purposely made bad.x
    FlumeConfiguration.get().set(FlumeConfiguration.AGENT_LOG_DIR_NEW,
        "/tmp/baddirfoobarama");
    File f = new File("/tmp/baddirfoobarama");
    f.deleteOnExit();
    f.createNewFile(); // create as new empty file.

    final String[] simple = {
        "-1",
        "-n",
        "test",
        "-c",
        "test: text(\""
            + getClass().getClassLoader()
                .getResource(ExampleData.APACHE_REGEXES).getFile()
            + "\") | null;" };
    FlumeNode.setup(simple);
  }

  @Test
  public void testBadPerms() throws IOException, InterruptedException {
    Assume.assumeTrue(!OSUtils.isWindowsOS());
    try {
      // set log dir to a place where permissions should fail.
      FlumeConfiguration.get().set(FlumeConfiguration.AGENT_LOG_DIR_NEW,
          "/baddirfoobarama");

      final String[] simple = {
          "-1",
          "-n",
          "test",
          "-c",
          "test: text(\""
              + getClass().getClassLoader()
                  .getResource(ExampleData.APACHE_REGEXES).getFile()
              + "\") | null;" };
      FlumeNode.setup(simple);
    } catch (IOException e) {
      return;
    }
    fail("expected IOException");
  }
}
