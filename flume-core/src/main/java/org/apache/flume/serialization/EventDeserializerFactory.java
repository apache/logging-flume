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
import java.util.Locale;
import org.apache.flume.Context;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.spi.ComponentRegistry;
import org.apache.flume.spi.EventDeserializerProvider;

@InterfaceAudience.Private
@InterfaceStability.Stable
@ServiceConsumer(
        value = EventDeserializerProvider.class,
        resolution = Resolution.OPTIONAL,
        cardinality = Cardinality.MULTIPLE)
public class EventDeserializerFactory {

    @SuppressWarnings("deprecation")
    private static final ComponentRegistry<EventDeserializer.Builder> REGISTRY = new ComponentRegistry<>(
            EventDeserializerProvider.class,
            EventDeserializer.Builder.class,
            "event deserializer",
            EventDeserializerFactory::getLegacyClassName);

    public static EventDeserializer getInstance(String deserializerType, Context context, ResettableInputStream in) {
        Preconditions.checkNotNull(deserializerType, "deserializer type must not be null");
        return REGISTRY.newInstance(deserializerType).build(context, in);
    }

    private static String getLegacyClassName(String type) {
        try {
            Class<?> builderClass =
                    EventDeserializerType.valueOf(type.toUpperCase(Locale.ROOT)).getBuilderClass();
            return builderClass != null ? builderClass.getName() : null;
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
