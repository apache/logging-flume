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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
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
   * <tt>DEFAULT</tt> for the default client).
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
   * Delegates to {@link #getInstance(Properties props)}, given a File path
   * to a {@link Properties} file.
   * @param propertiesFile Valid properties file
   * @return RpcClient configured according to the given Properties file.
   * @throws FileNotFoundException If the file cannot be found
   * @throws IOException If there is an IO error
   */
  public static RpcClient getInstance(File propertiesFile)
      throws FileNotFoundException, IOException {
    Reader reader = new FileReader(propertiesFile);
    Properties props = new Properties();
    props.load(reader);
    return getInstance(props);
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

    if (hostname == null) {
      throw new NullPointerException("hostname must not be null");
    }
    if (port == null) {
      throw new NullPointerException("port must not be null");
    }
    if (batchSize == null) {
      throw new NullPointerException("batchSize must not be null");
    }

    Properties props = new Properties();
    props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
    props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1",
        hostname + ":" + port.intValue());
    props.setProperty(RpcClientConfigurationConstants.CONFIG_BATCH_SIZE, batchSize.toString());
    NettyAvroRpcClient client = new NettyAvroRpcClient();
    client.configure(props);
    return client;
  }

  /**
   * Return an {@linkplain RpcClient} that uses Thrift for communicating with
   * the next hop. The next hop must have a ThriftSource listening on the
   * specified port.
   * @param hostname - The hostname of the next hop.
   * @param port - The port on which the ThriftSource is listening
   * @param batchSize - batch size of each transaction.
   * @return an {@linkplain RpcClient} which uses thrift configured with the
   * given parameters.
   */
  public static RpcClient getThriftInstance(String hostname, Integer port,
    Integer batchSize) {
    if (hostname == null) {
      throw new NullPointerException("hostname must not be null");
    }
    if (port == null) {
      throw new NullPointerException("port must not be null");
    }
    if (batchSize == null) {
      throw new NullPointerException("batchSize must not be null");
    }

    Properties props = new Properties();
    props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
    props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1",
      hostname + ":" + port.intValue());
    props.setProperty(RpcClientConfigurationConstants.CONFIG_BATCH_SIZE, batchSize.toString());
    ThriftRpcClient client = new ThriftRpcClient();
    client.configure(props);
    return client;
  }

  /**
   * Return an {@linkplain RpcClient} that uses Thrift for communicating with
   * the next hop. The next hop must have a ThriftSource listening on the
   * specified port. This will use the default batch size. See {@linkplain
   * RpcClientConfigurationConstants}
   * @param hostname - The hostname of the next hop.
   * @param port - The port on which the ThriftSource is listening
   * @return - An {@linkplain RpcClient} which uses thrift configured with the
   * given parameters.
   */
  public static RpcClient getThriftInstance(String hostname, Integer port) {
    return getThriftInstance(hostname, port, RpcClientConfigurationConstants
      .DEFAULT_BATCH_SIZE);
  }

  /**
   * Return an {@linkplain RpcClient} that uses Thrift for communicating with
   * the next hop.
   * @param props
   * @return - An {@linkplain RpcClient} which uses thrift configured with the
   * given parameters.
   */
  public static RpcClient getThriftInstance(Properties props) {
    props.setProperty(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE,
      ClientType.THRIFT.clientClassName);
    return getInstance(props);
  }

  public static enum ClientType {
    OTHER(null),
    DEFAULT(NettyAvroRpcClient.class.getCanonicalName()),
    DEFAULT_FAILOVER(FailoverRpcClient.class.getCanonicalName()),
    DEFAULT_LOADBALANCE(LoadBalancingRpcClient.class.getCanonicalName()),
    THRIFT(ThriftRpcClient.class.getCanonicalName());


    private final String clientClassName;

    private ClientType(String className) {
      this.clientClassName = className;
    }

    protected String getClientClassName() {
      return this.clientClassName;
    }

  }
}
