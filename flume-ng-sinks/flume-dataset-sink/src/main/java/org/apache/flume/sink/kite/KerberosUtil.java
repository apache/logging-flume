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

package org.apache.flume.sink.kite;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import java.io.File;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KerberosUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KerberosUtil.class);

  public static class SecurityException extends RuntimeException {
    private SecurityException(String message) {
      super(message);
    }

    private SecurityException(String message, Throwable cause) {
      super(message, cause);
    }

    private SecurityException(Throwable cause) {
      super(cause);
    }
  }

  public static UserGroupInformation proxyAs(String username,
                                             UserGroupInformation login) {
    Preconditions.checkArgument(username != null && !username.isEmpty(),
        "Invalid username: " + String.valueOf(username));
    Preconditions.checkArgument(login != null,
        "Cannot proxy without an authenticated user");

    // hadoop impersonation works with or without kerberos security
    return UserGroupInformation.createProxyUser(username, login);
  }

  /**
   * Static synchronized method for static Kerberos login. <br/>
   * Static synchronized due to a thundering herd problem when multiple Sinks
   * attempt to log in using the same principal at the same time with the
   * intention of impersonating different users (or even the same user).
   * If this is not controlled, MIT Kerberos v5 believes it is seeing a replay
   * attach and it returns:
   * <blockquote>Request is a replay (34) - PROCESS_TGS</blockquote>
   * In addition, since the underlying Hadoop APIs we are using for
   * impersonation are static, we define this method as static as well.
   *
   * @param principal
   *         Fully-qualified principal to use for authentication.
   * @param keytab
   *         Location of keytab file containing credentials for principal.
   * @return Logged-in user
   * @throws SecurityException
   *         if login fails.
   * @throws IllegalArgumentException
   *         if the principal or the keytab is not usable
   */
  public static synchronized UserGroupInformation login(String principal,
                                                 String keytab) {
    // resolve the requested principal, if it is present
    String finalPrincipal = null;
    if (principal != null && !principal.isEmpty()) {
      try {
        // resolves _HOST pattern using standard Hadoop search/replace
        // via DNS lookup when 2nd argument is empty
        finalPrincipal = SecurityUtil.getServerPrincipal(principal, "");
      } catch (IOException e) {
        throw new SecurityException(
            "Failed to resolve Kerberos principal", e);
      }
    }

    // check if there is a user already logged in
    UserGroupInformation currentUser = null;
    try {
      currentUser = UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      // not a big deal but this shouldn't typically happen because it will
      // generally fall back to the UNIX user
      LOG.debug("Unable to get login user before Kerberos auth attempt", e);
    }

    // if the current user is valid (matches the given principal) then use it
    if (currentUser != null) {
      if (finalPrincipal == null ||
          finalPrincipal.equals(currentUser.getUserName())) {
        LOG.debug("Using existing login for {}: {}",
            finalPrincipal, currentUser);
        return currentUser;
      } else {
        // be cruel and unusual when user tries to login as multiple principals
        // this isn't really valid with a reconfigure but this should be rare
        // enough to warrant a restart of the agent JVM
        // TODO: find a way to interrogate the entire current config state,
        // since we don't have to be unnecessarily protective if they switch all
        // HDFS sinks to use a different principal all at once.
        throw new SecurityException(
            "Cannot use multiple Kerberos principals: " + finalPrincipal +
                " would replace " + currentUser.getUserName());
      }
    }

    // prepare for a new login
    Preconditions.checkArgument(principal != null && !principal.isEmpty(),
        "Invalid Kerberos principal: " + String.valueOf(principal));
    Preconditions.checkNotNull(finalPrincipal,
        "Resolved principal must not be null");
    Preconditions.checkArgument(keytab != null && !keytab.isEmpty(),
        "Invalid Kerberos keytab: " + String.valueOf(keytab));
    File keytabFile = new File(keytab);
    Preconditions.checkArgument(keytabFile.isFile() && keytabFile.canRead(),
        "Keytab is not a readable file: " + String.valueOf(keytab));

    try {
      // attempt static kerberos login
      LOG.debug("Logging in as {} with {}", finalPrincipal, keytab);
      UserGroupInformation.loginUserFromKeytab(principal, keytab);
      return UserGroupInformation.getLoginUser();
    } catch (IOException e) {
      throw new SecurityException("Kerberos login failed", e);
    }
  }

  /**
   * Allow methods to act with the privileges of a login.
   *
   * If the login is null, the current privileges will be used.
   *
   * @param <T> The return type of the action
   * @param login UserGroupInformation credentials to use for action
   * @param action A PrivilegedExceptionAction to perform as another user
   * @return the T value returned by action.run()
   */
  public static <T> T runPrivileged(UserGroupInformation login,
                                    PrivilegedExceptionAction<T> action) {
    try {
      if (login == null) {
        return action.run();
      } else {
        return login.doAs(action);
      }
    } catch (IOException ex) {
      throw new DatasetIOException("Privileged action failed", ex);
    } catch (InterruptedException ex) {
      Thread.interrupted();
      throw new DatasetException(ex);
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }
}
