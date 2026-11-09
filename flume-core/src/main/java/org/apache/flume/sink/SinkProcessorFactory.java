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
import java.util.List;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.conf.ComponentWithClassName;
import org.apache.flume.conf.Configurables;
import org.apache.flume.conf.sink.SinkProcessorType;
import org.apache.flume.spi.ComponentRegistry;
import org.apache.flume.spi.SinkProcessorProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@ServiceConsumer(
        value = SinkProcessorProvider.class,
        resolution = Resolution.OPTIONAL,
        cardinality = Cardinality.MULTIPLE)
public class SinkProcessorFactory {
    private static final Logger logger = LogManager.getLogger();

    private static final String TYPE = "type";

    private static final String DEFAULT_TYPE = "default";

    @SuppressWarnings("deprecation")
    private static final ComponentRegistry<SinkProcessor> REGISTRY = new ComponentRegistry<>(
            SinkProcessorProvider.class,
            SinkProcessor.class,
            "sink processor",
            type -> ComponentWithClassName.getClassName(SinkProcessorType.class, type));

    /**
     * <p>
     * Creates a sink processor and configures it using the provided context
     * </p>
     *
     * @param context Context limited to that of the processor. Should include type
     * and any settings relevant to that processor type. Referer to javadoc for
     * specific sink
     * @param sinks A non-null, non-empty list of sinks to be assigned to the
     *  processor
     * @return A configured SinkProcessor
     * @see org.apache.flume.SinkProcessorType
     * @throws FlumeException Runtime exception thrown in the case of an invalid
     * processor configuration
     */
    public static SinkProcessor getProcessor(Context context, List<Sink> sinks) {
        Preconditions.checkNotNull(context);
        Preconditions.checkNotNull(sinks);
        Preconditions.checkArgument(!sinks.isEmpty());
        String typeStr = context.getParameters().get(TYPE);
        if (typeStr == null) {
            throw new FlumeException("Sink processor type not specified");
        }
        logger.debug("Creating instance of sink processor type {}", typeStr);
        SinkProcessor processor = REGISTRY.newInstance(typeStr);
        processor.setSinks(sinks);
        Configurables.configure(processor, context);
        return processor;
    }

    public static SinkProcessor getProcessor(ComponentConfiguration conf, List<Sink> sinks) {
        String typeStr = conf.getType();
        String type = DEFAULT_TYPE;
        if (typeStr != null) {
            try {
                REGISTRY.getComponentClass(typeStr);
                type = typeStr;
            } catch (FlumeException ex) {
                logger.warn("Sink processor type {} does not exist, using default", typeStr, ex);
            }
        }
        SinkProcessor processor = REGISTRY.newInstance(type);
        processor.setSinks(sinks);
        Configurables.configure(processor, conf);
        return processor;
    }
}
