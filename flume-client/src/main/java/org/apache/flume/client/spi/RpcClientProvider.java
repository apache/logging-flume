/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.client.spi;

import java.io.IOException;
import java.util.Properties;
import org.apache.flume.api.RpcClient;

/**
 * Creates the {@link RpcClient} instances of one client type.
 *
 * <p>Providers are discovered with {@link java.util.ServiceLoader}
 * and selected by the {@code client.type} configuration property:
 * register implementations in {@code META-INF/services/org.apache.flume.client.spi.RpcClientProvider}.
 */
public interface RpcClientProvider {

    /**
     * Returns the value of {@code client.type} that selects this provider.
     *
     * <p>Client types are compared case-insensitively.
     */
    String getName();

    /**
     * Creates a client configured with the given properties.
     *
     * @param properties the client configuration.
     * @return a new client, ready-to-send events.
     * @throws IllegalArgumentException if the configuration is invalid.
     * @throws IOException if the client fails to connect.
     */
    RpcClient create(Properties properties) throws IOException;
}
