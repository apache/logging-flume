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
package org.apache.flume.interceptor;

import aQute.bnd.annotation.Cardinality;
import aQute.bnd.annotation.Resolution;
import aQute.bnd.annotation.spi.ServiceConsumer;
import org.apache.flume.interceptor.Interceptor.Builder;
import org.apache.flume.spi.ComponentRegistry;
import org.apache.flume.spi.InterceptorProvider;

/**
 * Factory used to instantiate interceptor builders.
 */
@ServiceConsumer(
        value = InterceptorProvider.class,
        resolution = Resolution.OPTIONAL,
        cardinality = Cardinality.MULTIPLE)
public class InterceptorBuilderFactory {

    private static final ComponentRegistry<Builder> REGISTRY =
            new ComponentRegistry<>(InterceptorProvider.class, Builder.class, "interceptor");

    /**
     * Instantiate specified class, either alias or fully-qualified class name.
     *
     * @throws org.apache.flume.FlumeException if the type is unknown or the builder cannot be instantiated.
     */
    public static Builder newInstance(String name)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        return REGISTRY.newInstance(name);
    }
}
