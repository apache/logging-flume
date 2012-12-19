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
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
@InterfaceStability.Stable
public class EventDeserializerFactory {

  private static final Logger logger =
      LoggerFactory.getLogger(EventDeserializerFactory.class);

  public static EventDeserializer getInstance(
      String deserializerType, Context context, ResettableInputStream in) {

    Preconditions.checkNotNull(deserializerType,
        "serializer type must not be null");

    // try to find builder class in enum of known output serializers
    EventDeserializerType type;
    try {
      type = EventDeserializerType.valueOf(deserializerType.toUpperCase());
    } catch (IllegalArgumentException e) {
      logger.debug("Not in enum, loading builder class: {}", deserializerType);
      type = EventDeserializerType.OTHER;
    }
    Class<? extends EventDeserializer.Builder> builderClass =
        type.getBuilderClass();

    // handle the case where they have specified their own builder in the config
    if (builderClass == null) {
      try {
        Class c = Class.forName(deserializerType);
        if (c != null && EventDeserializer.Builder.class.isAssignableFrom(c)) {
          builderClass = (Class<? extends EventDeserializer.Builder>) c;
        } else {
          String errMessage = "Unable to instantiate Builder from " +
              deserializerType + ": does not appear to implement " +
              EventDeserializer.Builder.class.getName();
          throw new FlumeException(errMessage);
        }
      } catch (ClassNotFoundException ex) {
        logger.error("Class not found: " + deserializerType, ex);
        throw new FlumeException(ex);
      }
    }

    // build the builder
    EventDeserializer.Builder builder;
    try {
      builder = builderClass.newInstance();
    } catch (InstantiationException ex) {
      String errMessage = "Cannot instantiate builder: " + deserializerType;
      logger.error(errMessage, ex);
      throw new FlumeException(errMessage, ex);
    } catch (IllegalAccessException ex) {
      String errMessage = "Cannot instantiate builder: " + deserializerType;
      logger.error(errMessage, ex);
      throw new FlumeException(errMessage, ex);
    }

    return builder.build(context, in);
  }

}
