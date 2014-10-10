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
package org.apache.flume.serialization;

import com.google.common.base.Preconditions;
import java.io.OutputStream;
import java.util.Locale;

import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class EventSerializerFactory {

  private static final Logger logger =
      LoggerFactory.getLogger(EventSerializerFactory.class);

  public static EventSerializer getInstance(
      String serializerType, Context context, OutputStream out) {

    Preconditions.checkNotNull(serializerType,
        "serializer type must not be null");

    // try to find builder class in enum of known output serializers
    EventSerializerType type;
    try {
      type = EventSerializerType.valueOf(serializerType.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      logger.debug("Not in enum, loading builder class: {}", serializerType);
      type = EventSerializerType.OTHER;
    }
    Class<? extends EventSerializer.Builder> builderClass =
        type.getBuilderClass();

    // handle the case where they have specified their own builder in the config
    if (builderClass == null) {
      try {
        Class c = Class.forName(serializerType);
        if (c != null && EventSerializer.Builder.class.isAssignableFrom(c)) {
          builderClass = (Class<? extends EventSerializer.Builder>) c;
        } else {
          String errMessage = "Unable to instantiate Builder from " +
              serializerType + ": does not appear to implement " +
              EventSerializer.Builder.class.getName();
          throw new FlumeException(errMessage);
        }
      } catch (ClassNotFoundException ex) {
        logger.error("Class not found: " + serializerType, ex);
        throw new FlumeException(ex);
      }
    }

    // build the builder
    EventSerializer.Builder builder;
    try {
      builder = builderClass.newInstance();
    } catch (InstantiationException ex) {
      String errMessage = "Cannot instantiate builder: " + serializerType;
      logger.error(errMessage, ex);
      throw new FlumeException(errMessage, ex);
    } catch (IllegalAccessException ex) {
      String errMessage = "Cannot instantiate builder: " + serializerType;
      logger.error(errMessage, ex);
      throw new FlumeException(errMessage, ex);
    }

    return builder.build(context, out);
  }

}
