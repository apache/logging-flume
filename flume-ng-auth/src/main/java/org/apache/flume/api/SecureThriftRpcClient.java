/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.api;

import org.apache.flume.FlumeException;
import org.apache.flume.auth.FlumeAuthenticationUtil;
import org.apache.flume.auth.FlumeAuthenticator;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import javax.security.auth.callback.CallbackHandler;
import javax.security.sasl.Sasl;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class SecureThriftRpcClient extends ThriftRpcClient {

  private static final String CLIENT_PRINCIPAL = "client-principal";
  private static final String CLIENT_KEYTAB = "client-keytab";
  private static final String SERVER_PRINCIPAL = "server-principal";

  private String serverPrincipal;
  private FlumeAuthenticator privilegedExecutor;

  @Override
  protected void configure(Properties properties) throws FlumeException {
    super.configure(properties);
    serverPrincipal = properties.getProperty(SERVER_PRINCIPAL);
    if (serverPrincipal == null || serverPrincipal.isEmpty()) {
      throw new IllegalArgumentException("Flume in secure mode, but Flume config doesn't "
              + "specify a server principal to use for Kerberos auth.");
    }
    String clientPrincipal = properties.getProperty(CLIENT_PRINCIPAL);
    String keytab = properties.getProperty(CLIENT_KEYTAB);
    this.privilegedExecutor = FlumeAuthenticationUtil.getAuthenticator(clientPrincipal, keytab);
    if (!privilegedExecutor.isAuthenticated()) {
      throw new FlumeException("Authentication failed in Kerberos mode for " +
                               "principal " + clientPrincipal + " keytab " + keytab);
    }
  }

  @Override
  protected TTransport getTransport(TSocket tsocket) throws Exception {
    Map<String, String> saslProperties = new HashMap<String, String>();
    saslProperties.put(Sasl.QOP, "auth");
    String[] names;
    try {
      names = FlumeAuthenticationUtil.splitKerberosName(serverPrincipal);
    } catch (IOException e) {
      throw new FlumeException(
              "Error while trying to resolve Principal name - " + serverPrincipal, e);
    }
    return new UgiSaslClientTransport(
            "GSSAPI", null, names[0], names[1], saslProperties, null, tsocket, privilegedExecutor);
  }

  /**
   * This transport wraps the Sasl transports to set up the right UGI context for open().
   */
  public static class UgiSaslClientTransport extends TSaslClientTransport {
    PrivilegedExecutor privilegedExecutor;

    public UgiSaslClientTransport(String mechanism, String authorizationId,
                String protocol, String serverName, Map<String, String> props,
                CallbackHandler cbh, TTransport transport, PrivilegedExecutor privilegedExecutor)
        throws IOException {
      super(mechanism, authorizationId, protocol, serverName, props, cbh, transport);
      this.privilegedExecutor = privilegedExecutor;
    }

    /**
     * Open the SASL transport with using the current UserGroupInformation.
     * This is needed to get the current login context stored
     */
    @Override
    public void open() throws FlumeException {
      try {
        this.privilegedExecutor.execute(
            new PrivilegedExceptionAction<Void>() {
              public Void run() throws FlumeException {
                // this is a workaround to using UgiSaslClientTransport.super.open()
                // which results in IllegalAccessError
                callSuperClassOpen();
                return null;
              }
            });
      } catch (InterruptedException e) {
        throw new FlumeException("Interrupted while opening underlying transport", e);
      } catch (Exception e) {
        throw new FlumeException("Failed to open SASL transport", e);
      }
    }

    private void callSuperClassOpen() throws FlumeException {
      try {
        super.open();
      } catch (TTransportException e) {
        throw new FlumeException("Failed to open SASL transport", e);
      }
    }
  }
}
