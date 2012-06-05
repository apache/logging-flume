/*
 * Copyright 2012 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.api;

import java.net.InetSocketAddress;
import java.util.Properties;

import org.apache.flume.FlumeException;

/**
 * Factory class to construct Flume {@link RPCClient} implementations.
 */
public class RpcClientFactory {

  /**
   * Returns an instance of {@link RpcClient}, optionally with failover.
   * To create a failover client, the properties object should have a
   * property <tt>client.type</tt> which has the value "failover". The client
   * connects to hosts specified by <tt>hosts</tt> property in given properties.
   *
   * @see org.apache.flume.api.FailoverRpcClient
   * <p>
   * If no <tt>client.type</tt> is specified, a default client that connects to
   * single host at a given port is created.(<tt>type</tt> can also simply be
   * <tt>netty</tt> for the default client).
   *
   * @see org.apache.flume.api.NettyAvroClient
   *
   * @param properties The properties to instantiate the client with.
   * @throws FlumeException
   */

  @SuppressWarnings("unchecked")
  public static RpcClient getInstance(Properties properties)
      throws FlumeException {
    String type = null;
    type = properties.getProperty(
        RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE);
    if (type == null || type.isEmpty()) {
      type = ClientType.DEFAULT.getClientClassName();
    }
    Class<? extends AbstractRpcClient> clazz;
    AbstractRpcClient client;
    try {
      String clientClassType = type;
      ClientType clientType = null;
      try{
        clientType = ClientType.valueOf(type.toUpperCase());
      } catch (IllegalArgumentException e){
        clientType = ClientType.OTHER;
      }
      if (!clientType.equals(ClientType.OTHER)){
        clientClassType = clientType.getClientClassName();
      }
      clazz =
          (Class<? extends AbstractRpcClient>) Class.forName(clientClassType);
    } catch (ClassNotFoundException e) {
      throw new FlumeException("No such client!", e);
    }

    try {
      client = clazz.newInstance();
    } catch (InstantiationException e) {
      throw new FlumeException("Cannot instantiate client. " +
          "Exception follows:", e);
    } catch (IllegalAccessException e) {
      throw new FlumeException("Cannot instantiate client. " +
          "Exception follows:", e);
    }
    client.configure(properties);
    return client;

  }

  /**
   * Deprecated. Use
   * {@link getDefaultInstance() getDefaultInstance(String, Integer)} instead.
   * @throws FlumeException
   * @deprecated
   */
  @Deprecated
  public static RpcClient getInstance(String hostname, Integer port)
      throws FlumeException {
    return getDefaultInstance(hostname, port);
  }

  /**
   * Returns an instance of {@link RpcClient} connected to the specified
   * {@code hostname} and {@code port}.
   * @throws FlumeException
   */
  public static RpcClient getDefaultInstance(String hostname, Integer port)
      throws FlumeException {
    return getDefaultInstance(hostname, port, 0);

  }

  /**
   * Deprecated. Use
   * {@link getDefaultInstance() getDefaultInstance(String, Integer, Integer)}
   * instead.
   * @throws FlumeException
   * @deprecated
   */
  @Deprecated
  public static RpcClient getInstance(String hostname, Integer port,
      Integer batchSize) throws FlumeException {
    return getDefaultInstance(hostname, port, batchSize);
  }

  /**
   * Returns an instance of {@link RpcClient} connected to the specified
   * {@code hostname} and {@code port} with the specified {@code batchSize}.
   * @throws FlumeException
   */
  public static RpcClient getDefaultInstance(String hostname, Integer port,
      Integer batchSize) throws FlumeException {
    NettyAvroRpcClient client = new NettyAvroRpcClient(
        new InetSocketAddress(hostname, port), batchSize);
    return client;

  }

  public static enum ClientType {
    OTHER(null),
    DEFAULT("org.apache.flume.api.NettyAvroRpcClient"),
    DEFAULT_FAILOVER("org.apache.flume.api.FailoverRpcClient");

    private final String clientClassName;

    private ClientType(String className) {
      this.clientClassName = className;
  }

    protected String getClientClassName() {
      return this.clientClassName;
    }

  }
}
