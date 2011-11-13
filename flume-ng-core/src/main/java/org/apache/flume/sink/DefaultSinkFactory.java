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
import java.util.Set;

import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class DefaultSinkFactory implements SinkFactory {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultSinkFactory.class);

  public Map<String, Class<? extends Sink>> sinkRegistry;

  public DefaultSinkFactory() {
    sinkRegistry = new HashMap<String, Class<? extends Sink>>();
  }

  @Override
  public boolean register(String name, Class<? extends Sink> sinkClass) {
    logger.info("Register sink name:{} class:{}", name, sinkClass);

    if (sinkRegistry.containsKey(name)) {
      return false;
    }

    sinkRegistry.put(name, sinkClass);
    return true;
  }

  @Override
  public boolean unregister(String name) {
    logger.info("Unregister source class:{}", name);

    return sinkRegistry.remove(name) != null;
  }

  @Override
  public Set<String> getSinkNames() {
    return sinkRegistry.keySet();
  }

  @Override
  public Sink create(String name) throws InstantiationException {
    Preconditions.checkNotNull(name);

    logger.debug("Creating instance of sink {}", name);

    if (!sinkRegistry.containsKey(name)) {
      return null;
    }

    Sink sink = null;

    try {
      sink = sinkRegistry.get(name).newInstance();
    } catch (IllegalAccessException e) {
      throw new InstantiationException("Unable to create sink " + name
          + " due to " + e.getMessage());
    }

    return sink;
  }

  @Override
  public String toString() {
    return "{ sinkRegistry:" + sinkRegistry + " }";
  }

  public Map<String, Class<? extends Sink>> getSinkRegistry() {
    return sinkRegistry;
  }

  public void setSinkRegistry(
      Map<String, Class<? extends Sink>> sinkRegistry) {
    this.sinkRegistry = sinkRegistry;
  }

}
