/**
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
package org.apache.flume.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

import static org.apache.hadoop.fs.CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION;

/**
 * A kerberos authenticator, which authenticates using the supplied principal
 * and keytab and executes with  authenticated privileges
 */
class KerberosAuthenticator implements FlumeAuthenticator {

  private static final Logger LOG = LoggerFactory
          .getLogger(KerberosAuthenticator.class);

  private volatile UserGroupInformation ugi;
  private volatile KerberosUser prevUser;
  private volatile PrivilegedExecutor privilegedExecutor;
  private Map<String, PrivilegedExecutor> proxyCache = new HashMap<String, PrivilegedExecutor>();


  @Override
  public  <T> T execute(PrivilegedAction<T> action) {
    return privilegedExecutor.execute(action);
  }

  @Override
  public <T> T execute(PrivilegedExceptionAction<T> action) throws Exception {
    return privilegedExecutor.execute(action);
  }

  @Override
  public synchronized PrivilegedExecutor proxyAs(String proxyUserName) {
    if (proxyUserName == null || proxyUserName.isEmpty()) {
      return this;
    }
    if (proxyCache.get(proxyUserName) == null) {
      UserGroupInformation proxyUgi;
      proxyUgi = UserGroupInformation.createProxyUser(proxyUserName, ugi);
      printUGI(proxyUgi);
      proxyCache.put(proxyUserName, new UGIExecutor(proxyUgi));
    }
    return proxyCache.get(proxyUserName);
  }

  @Override
  public boolean isAuthenticated() {
    return true;
  }

  /**
   * When valid principal and keytab are provided and if authentication has
   * not yet been done for this object, this method authenticates the
   * credentials and populates the ugi. In case of null or invalid credentials
   * IllegalArgumentException is thrown. In case of failure to authenticate,
   * SecurityException is thrown. If authentication has already happened on
   * this KerberosAuthenticator object, then this method checks to see if the current
   * credentials passed are same as the validated credentials. If not, it throws
   * an exception as this authenticator can represent only one Principal.
   *
   * @param principal
   * @param keytab
   */
  public synchronized void authenticate(String principal, String keytab) {
    // sanity checking

    Preconditions.checkArgument(principal != null && !principal.isEmpty(),
            "Invalid Kerberos principal: " + String.valueOf(principal));
    Preconditions.checkArgument(keytab != null && !keytab.isEmpty(),
            "Invalid Kerberos keytab: " + String.valueOf(keytab));
    File keytabFile = new File(keytab);
    Preconditions.checkArgument(keytabFile.isFile() && keytabFile.canRead(),
            "Keytab is not a readable file: " + String.valueOf(keytab));


    // resolve the requested principal
    String resolvedPrincipal;
    try {
      // resolves _HOST pattern using standard Hadoop search/replace
      // via DNS lookup when 2nd argument is empty
      resolvedPrincipal = SecurityUtil.getServerPrincipal(principal, "");
    } catch (IOException e) {
      throw new IllegalArgumentException("Host lookup error resolving kerberos principal ("
              + principal + "). Exception follows.", e);
    }
    Preconditions.checkNotNull(resolvedPrincipal,
            "Resolved Principal must not be null");


    // be cruel and unusual when user tries to login as multiple principals
    // this isn't really valid with a reconfigure but this should be rare
    // enough to warrant a restart of the agent JVM
    // TODO: find a way to interrogate the entire current config state,
    // since we don't have to be unnecessarily protective if they switch all
    // HDFS sinks to use a different principal all at once.

    KerberosUser newUser = new KerberosUser(resolvedPrincipal, keytab);
    Preconditions.checkState(prevUser == null || prevUser.equals(newUser),
        "Cannot use multiple kerberos principals in the same agent. " +
        " Must restart agent to use new principal or keytab. " +
        "Previous = %s, New = %s", prevUser, newUser);


    // enable the kerberos mode of UGI, before doing anything else
    if (!UserGroupInformation.isSecurityEnabled()) {
      Configuration conf = new Configuration(false);
      conf.set(HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      UserGroupInformation.setConfiguration(conf);
    }

    // We are interested in currently logged in user with kerberos creds
    UserGroupInformation curUser = null;
    try {
      curUser = UserGroupInformation.getLoginUser();
      if (curUser != null && !curUser.hasKerberosCredentials()) {
        curUser = null;
      }
    } catch (IOException e) {
      LOG.warn("User unexpectedly had no active login. Continuing with " +
              "authentication", e);
    }

    /*
     *  if ugi is not null,
     *     if ugi matches currently logged in kerberos user, we are good
     *     else we are logged out, so relogin our ugi
     *  else if ugi is null, login and populate state
     */
    try {
      if (ugi != null) {
        if (curUser != null && curUser.getUserName().equals(ugi.getUserName())) {
          LOG.debug("Using existing principal login: {}", ugi);
        } else {
          LOG.info("Attempting kerberos Re-login as principal ({}) ",
                   new Object[] { ugi.getUserName() } );
          ugi.reloginFromKeytab();
        }
      } else {
        LOG.info("Attempting kerberos login as principal ({}) from keytab " +
                "file ({})", new Object[] { resolvedPrincipal, keytab } );
        UserGroupInformation.loginUserFromKeytab(resolvedPrincipal, keytab);
        this.ugi = UserGroupInformation.getLoginUser();
        this.prevUser = new KerberosUser(resolvedPrincipal, keytab);
        this.privilegedExecutor = new UGIExecutor(this.ugi);
      }
    } catch (IOException e) {
      throw new SecurityException("Authentication error while attempting to "
        + "login as kerberos principal (" + resolvedPrincipal + ") using "
        + "keytab (" + keytab + "). Exception follows.", e);
    }

    printUGI(this.ugi);
  }

  private void printUGI(UserGroupInformation ugi) {
    if (ugi != null) {
      // dump login information
      AuthenticationMethod authMethod = ugi.getAuthenticationMethod();
      LOG.info("\n{} \nUser: {} \nAuth method: {} \nKeytab: {} \n",
          new Object[] {
            authMethod.equals(AuthenticationMethod.PROXY) ? "Proxy as: " : "Logged as: ",
            ugi.getUserName(), authMethod, ugi.isFromKeytab()
          }
      );
    }
  }

  /**
   * startCredentialRefresher should be used only for long running
   * methods like Thrift source. For all privileged methods that use a UGI, the
   * credentials are checked automatically and refreshed before the
   * privileged method is executed in the UGIExecutor
   */
  @Override
  public void startCredentialRefresher() {
    int CHECK_TGT_INTERVAL = 120; // seconds
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    scheduler.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        try {
          ugi.checkTGTAndReloginFromKeytab();
        } catch (IOException e) {
          LOG.warn("Error occured during checkTGTAndReloginFromKeytab() for user " +
                  ugi.getUserName(), e);
        }
      }
    }, CHECK_TGT_INTERVAL, CHECK_TGT_INTERVAL, TimeUnit.SECONDS);
  }

  @VisibleForTesting
  String getUserName() {
    if (ugi != null) {
      return ugi.getUserName();
    } else {
      return null;
    }
  }
}



