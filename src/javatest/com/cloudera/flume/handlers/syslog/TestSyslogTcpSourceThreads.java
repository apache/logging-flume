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
package com.cloudera.flume.handlers.syslog;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

/**
 * Previously there were intermittent problems with opening and closing and
 * reopening this sink. These tests exercise those scenarios. (resulted in major
 * rewrite of multithreaded syslog tcp source.)
 * 
 * @author jon
 * 
 */
public class TestSyslogTcpSourceThreads {

  /*
   * A previous version with have problems when opening and closing this kind of
   * sink
   */
  @Test
  public void testOpenCloseSame() throws IOException {
    SyslogTcpSourceThreads svr = new SyslogTcpSourceThreads(5140);
    for (int i = 0; i < 100; i++) {
      SyslogTcpSourceThreads.LOG.info("same source opening " + i);
      svr.open();
      SyslogTcpSourceThreads.LOG.info("same source closing " + i);
      svr.close();
    }
  }

  @Test
  public void testOpenCloseNew() throws IOException {
    for (int i = 0; i < 100; i++) {
      SyslogTcpSourceThreads srv = new SyslogTcpSourceThreads(5140);
      SyslogTcpSourceThreads.LOG.info("new Source opening " + i);
      srv.open();
      SyslogTcpSourceThreads.LOG.info("new Source closing " + i);
      srv.close();
    }
  }

  public void testOpenMultipleSamePort() throws IOException {
    SyslogTcpSourceThreads srv1 = new SyslogTcpSourceThreads(5140);
    SyslogTcpSourceThreads srv2 = new SyslogTcpSourceThreads(5140);

    srv1.open();
    try {
      srv2.open();
    } catch (IOException e) {
      // Expect 1st to succeed but 2nd to fail because of port conflict
      return;
    } finally {
      srv2.close();
      srv1.close();
    }
    Assert.fail("this is not supposed to happen");
  }

  @Test
  public void testOpenMultipleDiffPort() throws IOException {
    SyslogTcpSourceThreads srv1 = new SyslogTcpSourceThreads(5140);
    SyslogTcpSourceThreads srv2 = new SyslogTcpSourceThreads(5141);

    try {
      srv1.open();
      srv2.open();
    } catch (IOException e) {
      Assert.fail("this is not supposed to happen");
    } finally {
      srv2.close();
      srv1.close();
    }

  }
}
