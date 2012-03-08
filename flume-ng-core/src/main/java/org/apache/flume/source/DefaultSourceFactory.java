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
package org.apache.flume.source;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class DefaultSourceFactory implements SourceFactory {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultSourceFactory.class);

  /**
   * Cache of sources created thus far. The outer map is keyed on the source
   * type and the inner map is keyed on source name.
   */
  private final Map<Class<?>, Map<String, Source>> sources;

  public DefaultSourceFactory() {
    sources = new HashMap<Class<?>, Map<String, Source>>();
  }

  @Override
  public synchronized boolean unregister(Source source) {
    Preconditions.checkNotNull(source);
    boolean removed = false;

    logger.debug("Unregistering source {}", source);

    Map<String, Source> sourceMap = sources.get(source.getClass());
    if (sourceMap != null) {
      removed = (sourceMap.remove(source.getName()) != null);

      if (sourceMap.size() == 0) {
        sources.remove(source.getClass());
      }
    }

    return removed;
  }

  @SuppressWarnings("unchecked")
  @Override
  public synchronized Source create(String name, String type)
      throws FlumeException {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(type);

    logger.debug("Creating instance of source {}, type {}", name, type);

    String sourceClassName = type;

    SourceType srcType = SourceType.OTHER;
    try {
      srcType = SourceType.valueOf(type.toUpperCase());
    } catch (IllegalArgumentException ex) {
      logger.debug("Source type {} is a custom type", type);
    }

    if (!srcType.equals(SourceType.OTHER)) {
      sourceClassName = srcType.getSourceClassName();
    }

    Class<? extends Source> sourceClass = null;
    try {
      sourceClass = (Class<? extends Source>) Class.forName(sourceClassName);
    } catch (Exception ex) {
      throw new FlumeException("Unable to load source type: " + type
          + ", class: " + sourceClassName, ex);
    }

    Map<String, Source> sourceMap = sources.get(sourceClass);
    if (sourceMap == null) {
      sourceMap = new HashMap<String, Source>();
      sources.put(sourceClass, sourceMap);
    }

    Source source = sourceMap.get(name);

    if (source == null) {
      try {
        source = sourceClass.newInstance();
        source.setName(name);
        sourceMap.put(name, source);
      } catch (Exception ex) {
        // Clean up the source map
        sources.remove(sourceClass);
        throw new FlumeException("Unable to create source: " + name
            +", type: " + type + ", class: " + sourceClassName, ex);
      }
    }

    return source;
  }

  public synchronized Map<Class<?>, Map<String, Source>> getRegistryClone() {
    Map<Class<?>, Map<String, Source>> result =
        new HashMap<Class<?>, Map<String, Source>>();

    for (Class<?> klass : sources.keySet()) {
      Map<String, Source> srcMap = sources.get(klass);
      Map<String, Source> resultSrcMap = new HashMap<String, Source>();
      resultSrcMap.putAll(srcMap);
      result.put(klass, resultSrcMap);
    }

    return result;
  }
}
