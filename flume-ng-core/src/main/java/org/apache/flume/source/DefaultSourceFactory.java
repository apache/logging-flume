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

import java.util.Locale;

import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.SourceFactory;
import org.apache.flume.conf.source.SourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class DefaultSourceFactory implements SourceFactory {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultSourceFactory.class);

  @Override
  public Source create(String name, String type) throws FlumeException {
    Preconditions.checkNotNull(name, "name");
    Preconditions.checkNotNull(type, "type");
    logger.info("Creating instance of source {}, type {}", name, type);
    Class<? extends Source> sourceClass = getClass(type);
    try {
      Source source = sourceClass.newInstance();
      source.setName(name);
      return source;
    } catch (Exception ex) {
      throw new FlumeException("Unable to create source: " + name
          + ", type: " + type + ", class: " + sourceClass.getName(), ex);
    }
  }
  @SuppressWarnings("unchecked")
  @Override
  public Class<? extends Source> getClass(String type) throws FlumeException {
    String sourceClassName = type;
    SourceType srcType = SourceType.OTHER;
    try {
      srcType = SourceType.valueOf(type.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException ex) {
      logger.debug("Source type {} is a custom type", type);
    }
    if (!srcType.equals(SourceType.OTHER)) {
      sourceClassName = srcType.getSourceClassName();
    }
    try {
      return (Class<? extends Source>) Class.forName(sourceClassName);
    } catch (Exception ex) {
      throw new FlumeException("Unable to load source type: " + type
          + ", class: " + sourceClassName, ex);
    }
  }
}
