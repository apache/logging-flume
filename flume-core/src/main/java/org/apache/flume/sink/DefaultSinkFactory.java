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
package org.apache.flume.sink;

import aQute.bnd.annotation.Cardinality;
import aQute.bnd.annotation.Resolution;
import aQute.bnd.annotation.spi.ServiceConsumer;
import com.google.common.base.Preconditions;
import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.apache.flume.conf.ComponentWithClassName;
import org.apache.flume.conf.sink.SinkType;
import org.apache.flume.spi.ComponentRegistry;
import org.apache.flume.spi.SinkProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@ServiceConsumer(value = SinkProvider.class, resolution = Resolution.OPTIONAL, cardinality = Cardinality.MULTIPLE)
public class DefaultSinkFactory implements SinkFactory {

    private static final Logger logger = LogManager.getLogger();

    @SuppressWarnings("deprecation")
    private static final ComponentRegistry<Sink> REGISTRY = new ComponentRegistry<>(
            SinkProvider.class, Sink.class, "sink", type -> ComponentWithClassName.getClassName(SinkType.class, type));

    @Override
    public Sink create(String name, String type) throws FlumeException {
        Preconditions.checkNotNull(name, "name");
        Preconditions.checkNotNull(type, "type");
        logger.info("Creating instance of sink {}, type {}", name, type);
        Sink sink = REGISTRY.newInstance(type);
        sink.setName(name);
        return sink;
    }

    @Override
    public Class<? extends Sink> getClass(String type) throws FlumeException {
        Preconditions.checkNotNull(type, "type");
        return REGISTRY.getComponentClass(type);
    }
}
