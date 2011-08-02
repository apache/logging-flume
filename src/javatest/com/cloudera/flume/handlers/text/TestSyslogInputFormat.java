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
package com.cloudera.flume.handlers.text;

import java.util.Calendar;
import java.util.Date;

import junit.framework.TestCase;

import com.cloudera.flume.core.Event;

/**
 * Simple tests for syslog file output. Extracts service if there a colon,
 * otherwise skips it.
 */
public class TestSyslogInputFormat extends TestCase {

  public void testSyslogExtractorService() throws EventExtractException {
    String test = "Aug 21 08:02:39 soundwave NetworkManager: <info>  (wlan0): supplicant connection state:  completed -> group handshake";

    SyslogEntryFormat ex = new SyslogEntryFormat();
    Event e = ex.extract(test, 2009);

    assertNotNull(e);
    System.out.println(e);

    assertEquals("soundwave", e.getHost());
    assertEquals("NetworkManager", new String(e.get("service")));
    assertEquals(test, new String(e.getBody()));

    Calendar c = Calendar.getInstance();
    c.set(2009, Calendar.AUGUST, 21, 8, 2, 39);
    c.set(Calendar.MILLISECOND, 0);
    System.out.printf("extracted %s expected %s\n", new Date(e.getTimestamp()),
        c.getTime());
    assertEquals(c.getTime().getTime(), e.getTimestamp());
  }

  public void testSyslogExtractorNoService() throws EventExtractException {
    String test = "Aug 21 08:03:14 soundwave last message repeated 2 times";
    SyslogEntryFormat ex = new SyslogEntryFormat();
    Event e = ex.extract(test, 2009);

    assertNotNull(e);
    System.out.println(e);

    assertEquals("soundwave", e.getHost());
    assertEquals(test, new String(e.getBody()));
    assertEquals(null, e.get("service"));

    Calendar c = Calendar.getInstance();
    c.set(2009, Calendar.AUGUST, 21, 8, 3, 14);
    c.set(Calendar.MILLISECOND, 0);
    System.out.printf("extracted %s expected %s\n", new Date(e.getTimestamp()),
        c.getTime());
    assertEquals(c.getTime().getTime(), e.getTimestamp());

  }

}
