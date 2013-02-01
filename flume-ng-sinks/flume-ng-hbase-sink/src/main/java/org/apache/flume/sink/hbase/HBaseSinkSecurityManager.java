/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.flume.sink.hbase;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import org.apache.flume.FlumeException;
import org.apache.flume.sink.hdfs.KerberosUser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to handle logging into HBase with the credentials passed in.
 */
public class HBaseSinkSecurityManager {

  /*
   * volatile for safe publication. Since this is updated only by
   * a single thread (configuration) and read later by the sink threads,
   * this can just be volatile, no need of Atomic reference.
   */
  private volatile static KerberosUser loggedInUser;
  private static final String FLUME_KEYTAB_KEY = "flume.keytab.key";
  private static final String FLUME_PRINCIPAL_KEY = "flume.principal.key";
  private static final Logger LOG =
          LoggerFactory.getLogger(HBaseSinkSecurityManager.class);

  /**
   * Checks if security is enabled for the HBase cluster.
   *
   * @return - true if security is enabled on the HBase cluster and
   * the underlying HDFS cluster.
   */
  public static boolean isSecurityEnabled(Configuration conf) {
    return User.isSecurityEnabled() && User.isHBaseSecurityEnabled(conf);
  }

  /**
   * Login the user using the configuration, and the hostname specified to use
   * for logging in.
   *
   * @param conf - Configuration to use for logging the user in.
   * @param hostname - The hostname to use for logging the user in. If no
   * hostname is specified (null or empty string), the canonical hostname for
   * the address returned by {@linkplain InetAddress#getLocalHost()} will be
   * used.
   * @return The logged in HBase {@linkplain User}.
   * @throws IOException if login failed, or hostname lookup failed.
   */
  public static synchronized User login(Configuration conf, String hostname,
          String kerberosPrincipal, String kerberosKeytab) throws IOException {
    if (kerberosPrincipal.isEmpty()) {
      String msg = "Login failed, since kerberos principal was not specified.";
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    if (kerberosKeytab.isEmpty()) {
      String msg = "Login failed, since kerberos keytab was not specified.";
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    } else {
      //If keytab is specified, user should want it take effect.
      //HDFSEventSink will halt when keytab file is non-exist or unreadable
      File kfile = new File(kerberosKeytab);
      if (!(kfile.isFile() && kfile.canRead())) {
        throw new IllegalArgumentException("The keyTab file: "
                + kerberosKeytab + " is nonexistent or can't read. "
                + "Please specify a readable keytab file for Kerberos auth.");
      }
    }
    String principal = kerberosPrincipal;
    try {
      // resolves _HOST pattern using standard Hadoop search/replace
      // via DNS lookup when 2nd argument is empty
      principal = SecurityUtil.getServerPrincipal(kerberosPrincipal,"");
    } catch (IOException e) {
      LOG.error("Host lookup error resolving kerberos principal ("
              + kerberosPrincipal + "). Exception follows.", e);
      throw e;
    }
    Preconditions.checkNotNull(principal, "Principal must not be null");
    KerberosUser newUser = new KerberosUser(principal, kerberosKeytab);
    //The HDFS Sink does not allow login credentials to change.
    //To be uniform, we will do the same thing here.
    User hbaseUser = null;
    boolean loggedIn = false;
    if (loggedInUser != null) {
      Preconditions.checkArgument(newUser.equals(loggedInUser),
              "Cannot switch kerberos credentials during a reconfiguration. "
              + "Please restart the agent to set the new credentials.");
      try {
        hbaseUser = User.create(UserGroupInformation.getLoginUser());
        loggedIn = true;
      } catch (IOException ex) {
        LOG.warn("Previous login does not exist, "
                + "will authenticate against KDC");
      }
    }
    if (!loggedIn) {
      if (hostname == null || hostname.isEmpty()) {
        hostname = InetAddress.getLocalHost().getCanonicalHostName();
      }
      conf.set(FLUME_KEYTAB_KEY, kerberosKeytab);
      conf.set(FLUME_PRINCIPAL_KEY, principal);
      User.login(conf, FLUME_KEYTAB_KEY, FLUME_PRINCIPAL_KEY, hostname);
      hbaseUser = User.create(UserGroupInformation.getLoginUser());
      loggedInUser = newUser;
      //TODO: Set the loggedInUser to the current user.
      LOG.info("Logged into HBase as user: " + hbaseUser.getName());
    }
    return hbaseUser;
  }
}
