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
package org.apache.flume.serialization;

import aQute.bnd.annotation.Cardinality;
import aQute.bnd.annotation.Resolution;
import aQute.bnd.annotation.spi.ServiceConsumer;
import com.google.common.base.Preconditions;
import java.io.OutputStream;
import java.util.Locale;
import org.apache.flume.Context;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.spi.ComponentRegistry;
import org.apache.flume.spi.EventSerializerProvider;

@InterfaceAudience.Private
@InterfaceStability.Stable
@ServiceConsumer(
        value = EventSerializerProvider.class,
        resolution = Resolution.OPTIONAL,
        cardinality = Cardinality.MULTIPLE)
public class EventSerializerFactory {

    @SuppressWarnings("deprecation")
    private static final ComponentRegistry<EventSerializer.Builder> REGISTRY = new ComponentRegistry<>(
            EventSerializerProvider.class,
            EventSerializer.Builder.class,
            "event serializer",
            EventSerializerFactory::getLegacyClassName);

    public static EventSerializer getInstance(String serializerType, Context context, OutputStream out) {
        Preconditions.checkNotNull(serializerType, "serializer type must not be null");
        return REGISTRY.newInstance(serializerType).build(context, out);
    }

    private static String getLegacyClassName(String type) {
        try {
            Class<?> builderClass =
                    EventSerializerType.valueOf(type.toUpperCase(Locale.ROOT)).getBuilderClass();
            return builderClass != null ? builderClass.getName() : null;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
