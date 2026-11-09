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
package org.apache.flume.source;

import aQute.bnd.annotation.Cardinality;
import aQute.bnd.annotation.Resolution;
import aQute.bnd.annotation.spi.ServiceConsumer;
import com.google.common.base.Preconditions;
import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.SourceFactory;
import org.apache.flume.conf.ComponentWithClassName;
import org.apache.flume.conf.source.SourceType;
import org.apache.flume.spi.ComponentRegistry;
import org.apache.flume.spi.SourceProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@ServiceConsumer(value = SourceProvider.class, resolution = Resolution.OPTIONAL, cardinality = Cardinality.MULTIPLE)
public class DefaultSourceFactory implements SourceFactory {

    private static final Logger logger = LogManager.getLogger();

    @SuppressWarnings("deprecation")
    private static final ComponentRegistry<Source> REGISTRY = new ComponentRegistry<>(
            SourceProvider.class,
            Source.class,
            "source",
            type -> ComponentWithClassName.getClassName(SourceType.class, type));

    @Override
    public Source create(String name, String type) throws FlumeException {
        Preconditions.checkNotNull(name, "name");
        Preconditions.checkNotNull(type, "type");
        logger.info("Creating instance of source {}, type {}", name, type);
        Source source = REGISTRY.newInstance(type);
        source.setName(name);
        return source;
    }

    @Override
    public Class<? extends Source> getClass(String type) throws FlumeException {
        Preconditions.checkNotNull(type, "type");
        return REGISTRY.getComponentClass(type);
    }
}
