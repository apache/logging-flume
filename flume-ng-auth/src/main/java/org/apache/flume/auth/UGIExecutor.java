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
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

class UGIExecutor implements PrivilegedExecutor {
  private UserGroupInformation ugi;
  private static final long MIN_TIME_BEFORE_RELOGIN = 5 * 60 * 1000L;
  private volatile long lastReloginAttempt = 0;

  UGIExecutor(UserGroupInformation ugi) {
    this.ugi = ugi;
  }

  @Override
  public <T> T execute(PrivilegedAction<T> action) {
    ensureValidAuth();
    return ugi.doAs(action);
  }

  @Override
  public <T> T execute(PrivilegedExceptionAction<T> action) throws Exception {
    ensureValidAuth();
    return ugi.doAs(action);
  }

  private void ensureValidAuth() {
    reloginUGI(ugi);
    if (ugi.getAuthenticationMethod().equals(AuthenticationMethod.PROXY)) {
      reloginUGI(ugi.getRealUser());
    }
  }

  /*
   * lastReloginAttempt is introduced to avoid making the synchronized call
   *  ugi.checkTGTAndReloginFromKeytab() often, Hence this method is
   *  intentionally not synchronized, so that multiple threads can execute without
   *  the need to lock, which may result in an edge case where multiple threads
   *  simultaneously reading the lastReloginAttempt, and finding it > 5 minutes, can
   *  result in all of them attempting the checkTGT method, which is fine
   */
  private void reloginUGI(UserGroupInformation ugi) {
    try {
      if (ugi.hasKerberosCredentials()) {
        long now = System.currentTimeMillis();
        if (now - lastReloginAttempt < MIN_TIME_BEFORE_RELOGIN) {
          return;
        }
        lastReloginAttempt = now;
        ugi.checkTGTAndReloginFromKeytab();
      }
    } catch (IOException e) {
      throw new SecurityException("Error trying to relogin from keytab for user "
              + ugi.getUserName(), e);
    }
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
