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
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.SecurityUtil;

import javax.security.auth.callback.CallbackHandler;
import java.io.IOException;

/**
 * FlumeAuthentication utility class that provides methods to get an
 * Authenticator. If proper credentials are provided KerberosAuthenticator is
 * returned which can be used to execute as the authenticated principal ,
 * or else a SimpleAuthenticator which executes without any authentication
 */
public class FlumeAuthenticationUtil {

  private FlumeAuthenticationUtil() {}

  private static KerberosAuthenticator kerbAuthenticator;

  /**
   * If principal and keytab are null, this method returns a SimpleAuthenticator
   * which executes without authentication. If valid credentials are
   * provided KerberosAuthenitcator is returned which can be used to execute as
   * the authenticated principal. Invalid credentials result in
   * IllegalArgumentException and Failure to authenticate results in SecurityException
   *
   * @param principal
   * @param keytab
   * @return FlumeAuthenticator
   *
   * @throws org.apache.flume.auth.SecurityException
   */
  public static synchronized FlumeAuthenticator getAuthenticator(
          String principal, String keytab) throws SecurityException {

    if (principal == null && keytab == null) {
      return SimpleAuthenticator.getSimpleAuthenticator();
    }

    Preconditions.checkArgument(principal != null,
            "Principal can not be null when keytab is provided");
    Preconditions.checkArgument(keytab != null,
            "Keytab can not be null when Principal is provided");

    if (kerbAuthenticator == null) {
      kerbAuthenticator = new KerberosAuthenticator();
    }
    kerbAuthenticator.authenticate(principal, keytab);

    return kerbAuthenticator;
  }

  /**
   * Returns the standard SaslGssCallbackHandler from the hadoop common module
   *
   * @return CallbackHandler
   */
  public static CallbackHandler getSaslGssCallbackHandler() {
    return new SaslRpcServer.SaslGssCallbackHandler();
  }

  /**
   * Resolves the principal using Hadoop common's SecurityUtil and splits
   * the kerberos principal into three parts user name, host and kerberos realm
   *
   * @param principal
   * @return String[] of username, hostname and kerberos realm
   * @throws IOException
   */
  public static String[] splitKerberosName(String principal) throws IOException {
    String resolvedPrinc = SecurityUtil.getServerPrincipal(principal, "");
    return SaslRpcServer.splitKerberosName(resolvedPrinc);
  }

  @VisibleForTesting
  static void clearCredentials() {
    kerbAuthenticator = null;
  }
}






