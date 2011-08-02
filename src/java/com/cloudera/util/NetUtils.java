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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

/**
 * This is a gets the local machine's domain name.
 */
public class NetUtils {
  final public static Logger LOG = Logger.getLogger(NetUtils.class);
  // this is recorded locally
  private static String localhost;

  // statically initialize localhost.
  static {
    if (localhost == null) {
      try {
        localhost = InetAddress.getLocalHost().getCanonicalHostName();
      } catch (UnknownHostException e) {
        LOG.error("Unable to get canonical host name! " + e.getMessage(), e);
      }
    }
  }

  // this is really to avoid throwing an exception in the constructor.
  public static String localhost() {
    return localhost;
  }

  /**
   * This should only be used in test cases to force a particular host name.
   */
  public static void setLocalhost(String host) {
    localhost = host;
  }

  public static Pair<String, Integer> parseHostPortPair(String sock,
      int portDefault) {
    String[] parts = sock.split(":");
    int port = portDefault;
    if (parts.length > 1) {
      port = Integer.parseInt(parts[1]);
    }
    return new Pair<String, Integer>(parts[0], port);
  }

}
