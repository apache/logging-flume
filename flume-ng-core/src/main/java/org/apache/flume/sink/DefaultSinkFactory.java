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

import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.apache.flume.conf.sink.SinkType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class DefaultSinkFactory implements SinkFactory {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultSinkFactory.class);

  @Override
  public Sink create(String name, String type) throws FlumeException {
    Preconditions.checkNotNull(name, "name");
    Preconditions.checkNotNull(type, "type");
    logger.info("Creating instance of sink: {}, type: {}", name, type);
    Class<? extends Sink> sinkClass = getClass(type);
    try {
      Sink sink = sinkClass.newInstance();
      sink.setName(name);
      return sink;
    } catch (Exception ex) {
      throw new FlumeException("Unable to create sink: " + name
          + ", type: " + type + ", class: " + sinkClass.getName(), ex);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Class<? extends Sink> getClass(String type)
  throws FlumeException {
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
    try {
      return (Class<? extends Sink>) Class.forName(sinkClassName);
    } catch (Exception ex) {
      throw new FlumeException("Unable to load sink type: " + type
          + ", class: " + sinkClassName, ex);
    }
  }
}
