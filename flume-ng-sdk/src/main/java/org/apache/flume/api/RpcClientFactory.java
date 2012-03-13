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

import org.apache.flume.FlumeException;

/**
 * Factory class to construct Flume {@link RPCClient} implementations.
 */
public class RpcClientFactory {

  /**
   * Returns an instance of {@link RpcClient} connected to the specified
   * {@code hostname} and {@code port}.
   * @throws FlumeException
   */
  public static RpcClient getInstance(String hostname, Integer port)
      throws FlumeException {

    return new NettyAvroRpcClient.Builder()
        .hostname(hostname).port(port).build();
  }

  /**
   * Returns an instance of {@link RpcClient} connected to the specified
   * {@code hostname} and {@code port} with the specified {@code batchSize}.
   * @throws FlumeException
   */
  public static RpcClient getInstance(String hostname, Integer port,
      Integer batchSize) throws FlumeException {

    return new NettyAvroRpcClient.Builder()
        .hostname(hostname).port(port).batchSize(batchSize).build();
  }

}
