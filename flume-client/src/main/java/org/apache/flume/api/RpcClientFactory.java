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

import aQute.bnd.annotation.Cardinality;
import aQute.bnd.annotation.Resolution;
import aQute.bnd.annotation.spi.ServiceConsumer;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.TreeMap;
import org.apache.flume.client.spi.RpcClientProvider;

/**
 * Factory class to construct Flume {@link RpcClient} implementations.
 *
 * <p>The {@value RpcClientConfigurationConstants#CONFIG_CLIENT_TYPE} property selects the client:
 * either the name of a provider registered through {@link java.util.ServiceLoader},
 * or the fully qualified class name of an unregistered {@link RpcClientProvider}.
 */
@ServiceConsumer(value = RpcClientProvider.class, resolution = Resolution.OPTIONAL, cardinality = Cardinality.MULTIPLE)
public class RpcClientFactory {

    private RpcClientFactory() {}

    /**
     * Returns an instance of {@link RpcClient} configured with the given properties.
     *
     * <p>If {@value RpcClientConfigurationConstants#CONFIG_CLIENT_TYPE} is not specified,
     * a client of type {@value RpcClientConfigurationConstants#DEFAULT_CLIENT_TYPE} is created.
     *
     * @param properties The properties to instantiate the client with.
     * @throws IllegalArgumentException if the client type is unknown or the properties are invalid.
     * @throws IOException if the client fails to connect.
     */
    public static RpcClient getInstance(Properties properties) throws IOException {
        String type = properties.getProperty(RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE);
        if (type == null || type.isEmpty()) {
            type = RpcClientConfigurationConstants.DEFAULT_CLIENT_TYPE;
        }
        return getProvider(type).create(properties);
    }

    /**
     * Delegates to {@link #getInstance(Properties props)}, given a File path
     * to a {@link Properties} file.
     * @param propertiesFile Valid properties file
     * @return RpcClient configured according to the given Properties file.
     * @throws IOException If the file cannot be read or the client fails to connect
     */
    public static RpcClient getInstance(File propertiesFile) throws IOException {
        Properties props = new Properties();
        try (Reader reader = new FileReader(propertiesFile)) {
            props.load(reader);
        }
        return getInstance(props);
    }

    /**
     * Deprecated. Use
     * {@link #getDefaultInstance(String, Integer)} instead.
     * @throws IOException if the client fails to connect.
     * @deprecated
     */
    @Deprecated
    public static RpcClient getInstance(String hostname, Integer port) throws IOException {
        return getDefaultInstance(hostname, port);
    }

    /**
     * Returns an instance of {@link RpcClient} connected to the specified
     * {@code hostname} and {@code port}.
     * @throws IOException if the client fails to connect.
     */
    public static RpcClient getDefaultInstance(String hostname, Integer port) throws IOException {
        return getDefaultInstance(hostname, port, 0);
    }

    /**
     * Deprecated. Use
     * {@link #getDefaultInstance(String, Integer, Integer)}
     * instead.
     * @throws IOException if the client fails to connect.
     * @deprecated
     */
    @Deprecated
    public static RpcClient getInstance(String hostname, Integer port, Integer batchSize) throws IOException {
        return getDefaultInstance(hostname, port, batchSize);
    }

    /**
     * Returns an instance of {@link RpcClient} connected to the specified
     * {@code hostname} and {@code port} with the specified {@code batchSize}.
     * @throws IOException if the client fails to connect.
     */
    public static RpcClient getDefaultInstance(String hostname, Integer port, Integer batchSize) throws IOException {
        return getProvider(RpcClientConfigurationConstants.DEFAULT_CLIENT_TYPE)
                .create(singleHostProperties(hostname, port, batchSize));
    }

    /**
     * Returns an instance of {@link RpcClient} connected to the specified
     * {@code hostname} and {@code port} using Thrift.
     * @throws IOException if the client fails to connect.
     */
    public static RpcClient getThriftInstance(String hostname, Integer port, Integer batchSize) throws IOException {
        return getProvider(RpcClientConfigurationConstants.THRIFT_CLIENT_TYPE)
                .create(singleHostProperties(hostname, port, batchSize));
    }

    /**
     * Returns an instance of {@link RpcClient} connected to the specified
     * {@code hostname} and {@code port} using Thrift.
     * @throws IOException if the client fails to connect.
     */
    public static RpcClient getThriftInstance(String hostname, Integer port) throws IOException {
        return getThriftInstance(hostname, port, RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE);
    }

    /**
     * Returns an instance of {@link RpcClient} configured with the given properties using Thrift.
     * @throws IOException if the client fails to connect.
     */
    public static RpcClient getThriftInstance(Properties props) throws IOException {
        props.setProperty(
                RpcClientConfigurationConstants.CONFIG_CLIENT_TYPE, RpcClientConfigurationConstants.THRIFT_CLIENT_TYPE);
        return getInstance(props);
    }

    private static Properties singleHostProperties(String hostname, Integer port, Integer batchSize) {
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
        props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1", hostname + ":" + port.intValue());
        props.setProperty(RpcClientConfigurationConstants.CONFIG_BATCH_SIZE, batchSize.toString());
        return props;
    }

    /**
     * Returns the provider for a client type.
     *
     * <p>Registered providers are matched by name, ignoring case;
     * {@value RpcClientConfigurationConstants#DEFAULT_CLIENT_TYPE} is an alias of
     * {@value RpcClientConfigurationConstants#AVRO_CLIENT_TYPE}.
     * Otherwise, the type is the fully qualified class name of a provider.
     */
    static RpcClientProvider getProvider(String type) {
        String name = type.equalsIgnoreCase(RpcClientConfigurationConstants.DEFAULT_CLIENT_TYPE)
                ? RpcClientConfigurationConstants.AVRO_CLIENT_TYPE
                : type;
        Map<String, RpcClientProvider> providers = loadProviders();
        RpcClientProvider provider = providers.get(name.toLowerCase(Locale.ROOT));
        if (provider != null) {
            return provider;
        }
        try {
            Class<?> clazz = Class.forName(type, true, RpcClientFactory.class.getClassLoader());
            if (!RpcClientProvider.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException(
                        "Client type " + type + " does not implement " + RpcClientProvider.class.getName());
            }
            return (RpcClientProvider) clazz.getConstructor().newInstance();
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "Unknown client type " + type + ": add the artifact that provides it, or use one of "
                            + providers.keySet(),
                    e);
        } catch (InstantiationException
                | IllegalAccessException
                | InvocationTargetException
                | NoSuchMethodException e) {
            throw new IllegalArgumentException("Cannot instantiate client provider " + type, e);
        }
    }

    private static Map<String, RpcClientProvider> loadProviders() {
        Map<String, RpcClientProvider> providers = new TreeMap<>();
        for (RpcClientProvider provider :
                ServiceLoader.load(RpcClientProvider.class, RpcClientFactory.class.getClassLoader())) {
            String name = provider.getName().toLowerCase(Locale.ROOT);
            RpcClientProvider previous = providers.putIfAbsent(name, provider);
            if (previous != null) {
                throw new IllegalStateException("Client type " + name + " is provided by both "
                        + previous.getClass().getName() + " and "
                        + provider.getClass().getName());
            }
        }
        return providers;
    }
}
