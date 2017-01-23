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

import java.util.Properties;

/**
 * Factory class to construct Flume {@link RPCClient} implementations.
 */
public class SecureRpcClientFactory {

  /**
   * Return a secure {@linkplain org.apache.flume.api.RpcClient} that uses Thrift for communicating
   * with the next hop.
   * @param props
   * @return - An {@linkplain org.apache.flume.api.RpcClient} which uses thrift configured with the
   * given parameters.
   */
  public static RpcClient getThriftInstance(Properties props) {
    ThriftRpcClient client = new SecureThriftRpcClient();
    client.configure(props);
    return client;
  }
}
