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
package org.apache.flume.sink;

import java.util.Properties;

import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.api.SecureRpcClientFactory;

/**
 * <p>
 * A {@link org.apache.flume.Sink} implementation that can send events to an RPC server (such as
 * Flume's {@link org.apache.flume.source.ThriftSource}).
 * </p>
 * <p>
 * This sink forms one half of Flume's tiered collection support. Events sent to
 * this sink are transported over the network to the hostname / port pair using
 * the RPC implementation encapsulated in {@link RpcClient}.
 * The destination is an instance of Flume's
 * {@link org.apache.flume.source.ThriftSource}, which
 * allows Flume agents to forward to other Flume agents, forming a tiered
 * collection infrastructure. Of course, nothing prevents one from using this
 * sink to speak to other custom built infrastructure that implements the same
 * RPC protocol.
 * </p>
 * <p>
 * Events are taken from the configured {@link org.apache.flume.Channel} in batches of the
 * configured <tt>batch-size</tt>. The batch size has no theoretical limits
 * although all events in the batch <b>must</b> fit in memory. Generally, larger
 * batches are far more efficient, but introduce a slight delay (measured in
 * millis) in delivery. The batch behavior is such that underruns (i.e. batches
 * smaller than the configured batch size) are possible. This is a compromise
 * made to maintain low latency of event delivery. If the channel returns a null
 * event, meaning it is empty, the batch is immediately sent, regardless of
 * size. Batch underruns are tracked in the metrics. Empty batches do not incur
 * an RPC roundtrip.
 * </p>
 * <p>
 * <b>Configuration options</b>
 * </p>
 * <table>
 * <tr>
 * <th>Parameter</th>
 * <th>Description</th>
 * <th>Unit (data type)</th>
 * <th>Default</th>
 * </tr>
 * <tr>
 * <td><tt>hostname</tt></td>
 * <td>The hostname to which events should be sent.</td>
 * <td>Hostname or IP (String)</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>port</tt></td>
 * <td>The port to which events should be sent on <tt>hostname</tt>.</td>
 * <td>TCP port (int)</td>
 * <td>none (required)</td>
 * </tr>
 * <tr>
 * <td><tt>batch-size</tt></td>
 * <td>The maximum number of events to send per RPC.</td>
 * <td>events (int)</td>
 * <td>100</td>
 * </tr>
 * <tr>
 * <td><tt>connect-timeout</tt></td>
 * <td>Maximum time to wait for the first Avro handshake and RPC request</td>
 * <td>milliseconds (long)</td>
 * <td>20000</td>
 * </tr>
 * <tr>
 * <td><tt>request-timeout</tt></td>
 * <td>Maximum time to wait RPC requests after the first</td>
 * <td>milliseconds (long)</td>
 * <td>20000</td>
 * </tr>
 * </table>
 * <p>
 * <b>Metrics</b>
 * </p>
 * <p>
 * TODO
 * </p>
 */
public class ThriftSink extends AbstractRpcSink {
  @Override
  protected RpcClient initializeRpcClient(Properties props) {
    // Only one thread is enough, since only one sink thread processes
    // transactions at any given time. Each sink owns its own Rpc client.
    props.setProperty(RpcClientConfigurationConstants.CONFIG_CONNECTION_POOL_SIZE,
                      String.valueOf(1));
    boolean enableKerberos = Boolean.parseBoolean(
        props.getProperty(RpcClientConfigurationConstants.KERBEROS_KEY, "false"));
    if (enableKerberos) {
      return SecureRpcClientFactory.getThriftInstance(props);
    } else {
      props.setProperty(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE,
                        RpcClientFactory.ClientType.THRIFT.name());
      return RpcClientFactory.getInstance(props);
    }
  }
}
