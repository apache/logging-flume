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

import java.util.concurrent.TimeUnit;

/**
 * Configuration constants used by the RpcClient. These configuration keys
 * can be specified via a Properties object to the appropriate method of
 * RpcClientFactory in order to obtain a customized RPC client.
 */
public final class RpcClientConfigurationConstants {

  /**
   * Hosts configuration key to specify a space delimited list of named
   * hosts. For example:
   * <pre>
   * hosts = h1 h2
   * </pre>
   */
  public static final String CONFIG_HOSTS = "hosts";

  /**
   * Hosts prefix to specify address of a particular named host. For example
   * <pre>
   * hosts.h1 = server1.example.com:12121
   * hosts.h2 = server2.example.com:12121
   * </pre>
   */
  public static final String CONFIG_HOSTS_PREFIX = "hosts.";

  /**
   * Configuration key used to specify the batch size. Default batch size is
   * {@value DEFAULT_BATCH_SIZE}.
   */
  public static final String CONFIG_BATCH_SIZE = "batch-size";

  /**
   * Configuration key to specify connection timeout in milliseconds. The
   * default connection timeout is {@value DEFAULT_CONNECT_TIMEOUT_MILLIS}.
   */
  public static final String CONFIG_CONNECT_TIMEOUT = "connect-timeout";

  /**
   * Configuration key to specify request timeout in milliseconds. The
   * default request timeout is {@value DEFAULT_REQUEST_TIMEOUT_MILLIS}.
   */
  public static final String CONFIG_REQUEST_TIMEOUT = "request-timeout";

  /**
   * Default batch size.
   */
  public final static Integer DEFAULT_BATCH_SIZE = 100;

  /**
   * Default connection, handshake, and initial request timeout in milliseconds.
   */
  public final static long DEFAULT_CONNECT_TIMEOUT_MILLIS =
      TimeUnit.MILLISECONDS.convert(20, TimeUnit.SECONDS);

  /**
   * Default request timeout in milliseconds.
   */
  public final static long DEFAULT_REQUEST_TIMEOUT_MILLIS =
      TimeUnit.MILLISECONDS.convert(20, TimeUnit.SECONDS);

  /**
   * Maximum attempts to be made by the FailoverRpcClient in case of
   * failures.
   */
  public static final String CONFIG_MAX_ATTEMPTS = "max-attempts";

  /**
   * Configuration key to specify the RpcClient type to be used. The available
   * values are <tt>DEFAULT</tt> which results in the creation of a regular
   * <tt>NettyAvroRpcClient</tt> and <tt>DEFAULT_FAILOVER</tt> which results
   * in the creation of a failover client implementation on top of multiple
   * <tt>NettyAvroRpcClient</tt>s. The default value of this configuration
   * is {@value #DEFAULT_CLIENT_TYPE}.
   *
   */
  public static final String CONFIG_CLIENT_TYPE = "client.type";

  /**
   * The default client type to be created if no explicit type is specified.
   */
  public static final String DEFAULT_CLIENT_TYPE =
      RpcClientFactory.ClientType.DEFAULT.name();

  /**
   * The selector type used by the <tt>LoadBalancingRpcClient</tt>. This
   * value of this setting could be either <tt>round_robin</tt>,
   * <tt>random</tt>, or the fully qualified name class that implements the
   * <tt>LoadBalancingRpcClient.HostSelector</tt> interface.
   */
  public static final String CONFIG_HOST_SELECTOR =
      "host-selector";

  public static final String HOST_SELECTOR_ROUND_ROBIN = "ROUND_ROBIN";
  public static final String HOST_SELECTOR_RANDOM = "RANDOM";

  public static final String CONFIG_MAX_BACKOFF = "maxBackoff";
  public static final String CONFIG_BACKOFF = "backoff";
  public static final String DEFAULT_BACKOFF = "false";

  /**
   * Maximum number of connections each Thrift Rpc client can open to a given
   * host.
   */
  public static final String CONFIG_CONNECTION_POOL_SIZE = "maxConnections";
  public static final int DEFAULT_CONNECTION_POOL_SIZE = 5;

  /**
   * The following are const for the NettyAvro Client.  To enable compression
   * and set a compression level
   */
  public static final String CONFIG_COMPRESSION_TYPE = "compression-type";
  public static final String CONFIG_COMPRESSION_LEVEL = "compression-level";
  public static final int DEFAULT_COMPRESSION_LEVEL = 6;


  /**
   * Configuration constants for SSL support
   */
  public static final String CONFIG_SSL = "ssl";
  public static final String CONFIG_TRUST_ALL_CERTS = "trust-all-certs";
  public static final String CONFIG_TRUSTSTORE = "truststore";
  public static final String CONFIG_TRUSTSTORE_PASSWORD = "truststore-password";
  public static final String CONFIG_TRUSTSTORE_TYPE = "truststore-type";

  /**
   * Configuration constants for the NettyAvroRpcClient
   * NioClientSocketChannelFactory
   */
  public static final String MAX_IO_WORKERS = "maxIoWorkers";

  private RpcClientConfigurationConstants() {
    // disable explicit object creation
  }
}
