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
import java.util.Set;

import org.apache.flume.Source;
import org.apache.flume.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class DefaultSourceFactory implements SourceFactory {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultSourceFactory.class);

  public Map<String, Class<? extends Source>> sourceRegistry;

  public DefaultSourceFactory() {
    sourceRegistry = new HashMap<String, Class<? extends Source>>();
  }

  @Override
  public boolean register(String name, Class<? extends Source> sourceClass) {
    logger.info("Register source name:{} class:{}", name, sourceClass);

    if (sourceRegistry.containsKey(name)) {
      return false;
    }

    sourceRegistry.put(name, sourceClass);
    return true;
  }

  @Override
  public boolean unregister(String name) {
    logger.info("Unregister source class:{}", name);

    return sourceRegistry.remove(name) != null;
  }

  @Override
  public Set<String> getSourceNames() {
    return sourceRegistry.keySet();
  }

  @Override
  public Source create(String name) throws InstantiationException {
    Preconditions.checkNotNull(name);

    logger.debug("Creating instance of source {}", name);

    /* FIXME: Is returning null really a good idea? Should we just panic? */
    if (!sourceRegistry.containsKey(name)) {
      return null;
    }

    Source source = null;

    try {
      source = sourceRegistry.get(name).newInstance();
    } catch (IllegalAccessException e) {
      throw new InstantiationException("Unable to create source " + name
          + " due to " + e.getMessage());
    }

    return source;
  }

  @Override
  public String toString() {
    return "{ sinkRegistry:" + sourceRegistry + " }";
  }

  public Map<String, Class<? extends Source>> getSourceRegistry() {
    return sourceRegistry;
  }

  public void setSourceRegistry(
      Map<String, Class<? extends Source>> sourceRegistry) {
    this.sourceRegistry = sourceRegistry;
  }

}
