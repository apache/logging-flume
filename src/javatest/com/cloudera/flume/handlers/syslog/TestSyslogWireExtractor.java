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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Arrays;

import junit.framework.TestCase;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.handlers.text.EventExtractException;

/**
 * This tests the wire extractors using the new parser.
 */
public class TestSyslogWireExtractor extends TestCase {
  /**
   * Test the extractor
   */
  public void testNewExtractor() throws EventExtractException {
    String msg = "This is a test";
    String entry = "<13>" + msg + "\n";
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(entry
        .getBytes()));
    Event e = SyslogWireExtractor.extractEvent(in);
    assertEquals(1, e.get(SyslogConsts.SYSLOG_FACILITY)[0]); // 1 is syslog
    assertEquals(5, e.get(SyslogConsts.SYSLOG_SEVERITY)[0]); // 5 is
    assertTrue(Arrays.equals(msg.getBytes(), e.getBody()));
  }

  /**
   * Extractors and formatters should be reversable
   */
  public void testNewFormatExtractor() throws EventExtractException {
    String msg = "Aug 21 08:02:39 soundwave NetworkManager: <info>  (wlan0): supplicant connection state:  completed -> group handshake";
    String entry = "<13>" + msg + "\n";

    DataInputStream in = new DataInputStream(new ByteArrayInputStream(entry
        .getBytes()));

    SyslogWireExtractor fmt = new SyslogWireExtractor();
    Event e = fmt.extract(in);
    System.out.printf("size entry: %d, size formatted: %d ",
        entry.getBytes().length, fmt.toBytes(e).length);
    assertTrue(Arrays.equals(entry.getBytes(), fmt.toBytes(e)));
  }

  /**
   * Extractors and formatters should be reversable
   */
  public void testNewFormatExtractor2() throws EventExtractException {
    // this is an example from beast.
    String msg = "Oct 14 22:12:49 Hostd: [2 009-10-14 22:12:49.912 100C1B90 verbose 'Cimsvc' ] Ticket issued for CIMOM version 1.0, user root";
    String entry = "<166>" + msg + "\n";

    DataInputStream in = new DataInputStream(new ByteArrayInputStream(entry
        .getBytes()));

    SyslogWireExtractor fmt = new SyslogWireExtractor();
    Event e = fmt.extract(in);
    assertTrue(Arrays.equals(entry.getBytes(), fmt.toBytes(e)));

  }

  /**
   * Extractors and formatters should be reversable
   */
  public void testNewFormatExtractor3() throws EventExtractException {
    // this is an example from beast.
    String msg = "Oct 15 01:04:15 Hostd: [2009-10-15 01:04:15.484 17FA5B90 verbose 'vm:/vmfs/volumes/4acaa2a2-a85a3928-97b1-003048c93e5f/Centos 386 Build01/Centos 386 Build01.vmx'] Updating current heartbeatStatus: greenellow2e09476e6b0]5285369c-0d52-ca5a-594e-f1de890";
    String entry = "<166>" + msg + "\n";

    DataInputStream in = new DataInputStream(new ByteArrayInputStream(entry
        .getBytes()));

    SyslogWireExtractor fmt = new SyslogWireExtractor();
    Event e = fmt.extract(in);
    assertTrue(Arrays.equals(entry.getBytes(), fmt.toBytes(e)));

  }

  public void testNewFail() {
    String msg = "this will fail";
    Event e = null;
    try {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(msg
          .getBytes()));

      e = SyslogWireExtractor.extractEvent(in);
      System.out.println(e);
    } catch (EventExtractException e1) {
      System.out.println("expected:" + e1);
      return; // success!
    }
    fail("Should have thrown exception");
  }

}
