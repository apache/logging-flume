/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.sink;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class DefaultSinkFactory implements SinkFactory {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultSinkFactory.class);

  private final Map<Class<?>, Map<String, Sink>> sinks;

  public DefaultSinkFactory() {
    sinks = new HashMap<Class<?>, Map<String, Sink>>();
  }

  @Override
  public synchronized boolean unregister(Sink sink) {
    Preconditions.checkNotNull(sink);
    boolean removed = false;

    logger.debug("Unregistering sink {}", sink);

    Map<String, Sink> sinkMap = sinks.get(sink.getClass());
    if (sinkMap != null) {
      removed = (sinkMap.remove(sink.getName()) != null);

      if (sinkMap.size() == 0) {
        sinks.remove(sink.getClass());
      }
    }

    return removed;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Sink create(String name, String type)
      throws FlumeException {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(type);
    logger.info("Creating instance of sink {} type{}", name, type);

    String sinkClassName = type;

    SinkType sinkType = SinkType.OTHER;
    try {
      sinkType = SinkType.valueOf(type.toUpperCase());
    } catch (IllegalArgumentException ex) {
      logger.debug("Sink type {} is a custom type", type);
    }

    if (!sinkType.equals(SinkType.OTHER)) {
      sinkClassName = sinkType.getSinkClassName();
    }

    Class<? extends Sink> sinkClass = null;
    try {
      sinkClass = (Class<? extends Sink>) Class.forName(sinkClassName);
    } catch (Exception ex) {
      throw new FlumeException("Unable to load sink type: " + type
          + ", class: " + sinkClassName, ex);
    }

    Map<String, Sink> sinkMap = sinks.get(sinkClass);
    if (sinkMap == null) {
      sinkMap = new HashMap<String, Sink>();
      sinks.put(sinkClass, sinkMap);
    }

    Sink sink = sinkMap.get(name);

    if (sink == null) {
      try {
        sink = sinkClass.newInstance();
        sink.setName(name);
        sinkMap.put(name,  sink);
      } catch (Exception ex) {
        // Clean up the sink map
        sinks.remove(sinkClass);
        throw new FlumeException("Unable to create sink: " + name
            + ", type: " + type + ", class: " + sinkClassName, ex);
      }
    }

    return sink;
  }

  public synchronized Map<Class<?>, Map<String, Sink>> getRegistryClone() {
    Map<Class<?>, Map<String, Sink>> result =
        new HashMap<Class<?>, Map<String, Sink>>();

    for (Class<?> klass : sinks.keySet()) {
      Map<String, Sink> sinkMap = sinks.get(klass);
      Map<String, Sink> resultSinkMap = new HashMap<String, Sink>();
      resultSinkMap.putAll(sinkMap);
      result.put(klass, resultSinkMap);
    }

    return result;
  }
}
