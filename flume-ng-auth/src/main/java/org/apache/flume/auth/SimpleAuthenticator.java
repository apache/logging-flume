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

import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;

/**
 * A no-op authenticator, which does not authenticate and executes
 * without any authenticated privileges
 */
class SimpleAuthenticator implements FlumeAuthenticator {
  private SimpleAuthenticator() {}

  private static class SimpleAuthenticatorHolder {
    public static SimpleAuthenticator authenticator = new SimpleAuthenticator();
  }

  public static SimpleAuthenticator getSimpleAuthenticator() {
    return SimpleAuthenticatorHolder.authenticator;
  }

  private Map<String, PrivilegedExecutor> proxyCache =
          new HashMap<String, PrivilegedExecutor>();


  @Override
  public <T> T execute(PrivilegedExceptionAction<T> action)
          throws Exception {
    return action.run();
  }

  @Override
  public <T> T execute(PrivilegedAction<T> action) {
    return action.run();
  }

  @Override
  public synchronized PrivilegedExecutor proxyAs(String proxyUserName) {
    if (proxyUserName == null || proxyUserName.isEmpty()) {
      return this;
    }
    if (proxyCache.get(proxyUserName) == null) {
      UserGroupInformation proxyUgi;
      try {
        proxyUgi = UserGroupInformation.createProxyUser(proxyUserName,
                UserGroupInformation.getCurrentUser());
      } catch (IOException e) {
        throw new SecurityException("Unable to create proxy User", e);
      }
      proxyCache.put(proxyUserName, new UGIExecutor(proxyUgi));
    }
    return proxyCache.get(proxyUserName);
  }

  @Override
  public boolean isAuthenticated() {
    return false;
  }

  @Override
  public void startCredentialRefresher() {
    // no-op
  }

}


