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
package com.cloudera.util;

import static org.junit.Assert.assertEquals;

import java.net.SocketException;
import java.net.UnknownHostException;

import org.junit.Test;

/**
 * This tests some of utility functions found in NetUtils.
 */
public class TestNetUtils {

  @Test
  public void testIsLocalHost() throws UnknownHostException, SocketException {
    assertEquals(0, NetUtils.findHostIndex(new String[] { "127.0.0.1" }));
    assertEquals(0, NetUtils.findHostIndex(new String[] { "localhost" }));
    assertEquals(0, NetUtils.findHostIndex(new String[] { "127.1.0.1" }));
  }

  /**
   * This test assumes that we can talk to a DNS server and look up the names
   * specified below.
   */
  @Test
  public void testIsLocalHostIdx() throws UnknownHostException, SocketException {
    assertEquals(1, NetUtils.findHostIndex(new String[] { "google.com",
        "127.0.0.1" }));
    assertEquals(2, NetUtils.findHostIndex(new String[] { "microsoft.com",
        "twitter.com", "localhost" }));
    assertEquals(0, NetUtils.findHostIndex(new String[] { "127.1.0.1",
        "cloudera.com", "yahoo.com" }));
    // all the specified names are not local, thus return -1
    assertEquals(-1, NetUtils.findHostIndex(new String[] { "apache.org",
        "cloudera.com", "yahoo.com" }));
  }

  @Test(expected = UnknownHostException.class)
  public void testBadHost() throws UnknownHostException, SocketException {
    NetUtils.findHostIndex(new String[] { "baddnsname" });
  }
}
