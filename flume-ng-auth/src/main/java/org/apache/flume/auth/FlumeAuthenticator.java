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

/**
 * FlumeAuthenticator extends on a PrivilegedExecutor providing capabilities to
 * proxy as a different user
 */
public interface FlumeAuthenticator extends PrivilegedExecutor {
  /**
   * Returns the current instance if proxyUsername is null or
   * returns the proxied Executor if proxyUserName is valid
   * @param proxyUserName
   * @return PrivilegedExecutor
   */
  public PrivilegedExecutor proxyAs(String proxyUserName);

  /**
   * Returns true, if the underlying Authenticator was obtained by
   * successful kerberos authentication
   * @return boolean
   */
  public boolean isAuthenticated();

  /**
   * For Authenticators backed by credentials, this method refreshes the
   * credentials periodically
   */
  public void startCredentialRefresher();
}
