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
package org.apache.flume.spi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Locale;
import java.util.ServiceLoader;
import java.util.function.Function;
import org.apache.flume.conf.channel.ChannelSelectorType;
import org.apache.flume.conf.channel.ChannelType;
import org.apache.flume.conf.sink.SinkProcessorType;
import org.apache.flume.conf.sink.SinkType;
import org.apache.flume.conf.source.SourceType;
import org.apache.flume.formatter.output.PathManagerType;
import org.apache.flume.interceptor.InterceptorType;
import org.apache.flume.serialization.EventDeserializerType;
import org.apache.flume.serialization.EventSerializerType;
import org.junit.Test;

/**
 * Checks that the providers in {@code flume-core} use the same names and classes as the legacy type enums.
 */
public class TestCoreComponentProviders {

    @Test
    public void sources() {
        check(SourceProvider.class, 3, name -> SourceType.valueOf(name).getClassName());
    }

    @Test
    public void sinks() {
        check(SinkProvider.class, 3, name -> SinkType.valueOf(name).getClassName());
    }

    @Test
    public void channels() {
        check(ChannelProvider.class, 1, name -> ChannelType.valueOf(name).getClassName());
    }

    @Test
    public void channelSelectors() {
        check(ChannelSelectorProvider.class, 4, name -> ChannelSelectorType.valueOf(name)
                .getClassName());
    }

    @Test
    public void sinkProcessors() {
        check(SinkProcessorProvider.class, 3, name -> SinkProcessorType.valueOf(name)
                .getClassName());
    }

    @Test
    public void interceptors() {
        check(InterceptorProvider.class, InterceptorType.values().length, name -> InterceptorType.valueOf(name)
                .getBuilderClass()
                .getName());
    }

    @Test
    public void eventSerializers() {
        check(EventSerializerProvider.class, 2, name -> EventSerializerType.valueOf(name)
                .getBuilderClass()
                .getName());
    }

    @Test
    public void eventDeserializers() {
        check(EventDeserializerProvider.class, 1, name -> EventDeserializerType.valueOf(name)
                .getBuilderClass()
                .getName());
    }

    @Test
    public void pathManagers() {
        check(PathManagerProvider.class, 2, name -> PathManagerType.valueOf(name)
                .getBuilderClass()
                .getName());
    }

    private static <P extends ComponentProvider<?>> void check(
            Class<P> providerType, int expectedCount, Function<String, String> legacyClassName) {
        int count = 0;
        for (P provider : ServiceLoader.load(providerType)) {
            String name = provider.getName();
            assertFalse(name, name.isEmpty());
            assertEquals(
                    name,
                    legacyClassName.apply(name.toUpperCase(Locale.ROOT)),
                    provider.getComponentClass().getName());
            count++;
        }
        assertEquals(providerType.getSimpleName(), expectedCount, count);
    }
}
