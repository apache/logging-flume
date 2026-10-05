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
package org.apache.flume.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Properties;
import org.apache.flume.client.spi.RpcClientProvider;
import org.apache.flume.event.Event;
import org.junit.Test;

public class TestRpcClientFactory {

    @Test
    public void testDefaultTypeIsAvro() throws Exception {
        assertSame(
                FakeAvroProvider.class, RpcClientFactory.getProvider("default").getClass());
        RpcClient client = RpcClientFactory.getInstance(new Properties());
        assertEquals("avro", ((FakeClient) client).type);
    }

    @Test
    public void testTypesAreCaseInsensitive() {
        assertSame(FakeAvroProvider.class, RpcClientFactory.getProvider("AvRo").getClass());
        assertSame(
                FailoverRpcClient.Provider.class,
                RpcClientFactory.getProvider("DEFAULT_FAILOVER").getClass());
        assertSame(
                LoadBalancingRpcClient.Provider.class,
                RpcClientFactory.getProvider("default_loadbalance").getClass());
    }

    @Test
    public void testUnregisteredProviderByClassName() throws Exception {
        Properties properties = new Properties();
        properties.setProperty(
                RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE, UnregisteredProvider.class.getName());
        RpcClient client = RpcClientFactory.getInstance(properties);
        assertEquals("unregistered", ((FakeClient) client).type);
    }

    @Test
    public void testUnknownType() {
        IllegalArgumentException e =
                assertThrows(IllegalArgumentException.class, () -> RpcClientFactory.getProvider("carrier-pigeon"));
        assertTrue(e.getMessage(), e.getMessage().contains("avro"));
        assertTrue(e.getMessage(), e.getMessage().contains("default_failover"));
    }

    @Test
    public void testClassThatIsNotAProvider() {
        assertThrows(IllegalArgumentException.class, () -> RpcClientFactory.getProvider(String.class.getName()));
    }

    /** Stands in for the provider of {@code flume-avro-client}. */
    public static final class FakeAvroProvider implements RpcClientProvider {

        @Override
        public String getName() {
            return "AVRO";
        }

        @Override
        public RpcClient create(Properties properties) {
            return new FakeClient("avro");
        }
    }

    /** A provider that is not registered with the {@link java.util.ServiceLoader}. */
    public static final class UnregisteredProvider implements RpcClientProvider {

        @Override
        public String getName() {
            return "unregistered";
        }

        @Override
        public RpcClient create(Properties properties) {
            return new FakeClient("unregistered");
        }
    }

    private static final class FakeClient implements RpcClient {

        private final String type;

        private FakeClient(String type) {
            this.type = type;
        }

        @Override
        public int getBatchSize() {
            return 1;
        }

        @Override
        public void append(Event event) {}

        @Override
        public void appendBatch(List<Event> events) {}

        @Override
        public boolean isActive() {
            return true;
        }

        @Override
        public void close() {}
    }
}
