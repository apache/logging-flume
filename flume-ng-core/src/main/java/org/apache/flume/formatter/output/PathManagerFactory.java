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
package org.apache.flume.formatter.output;

import com.google.common.base.Preconditions;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

/**
 * Create PathManager instances.
 */
public class PathManagerFactory {
  private static final Logger logger = LoggerFactory.getLogger(PathManagerFactory.class);

  public static PathManager getInstance(String managerType, Context context) {

    Preconditions.checkNotNull(managerType, "path manager type must not be null");

    // try to find builder class in enum of known output serializers
    PathManagerType type;
    try {
      type = PathManagerType.valueOf(managerType.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      logger.debug("Not in enum, loading builder class: {}", managerType);
      type = PathManagerType.OTHER;
    }
    Class<? extends PathManager.Builder> builderClass = type.getBuilderClass();

    // handle the case where they have specified their own builder in the config
    if (builderClass == null) {
      try {
        Class c = Class.forName(managerType);
        if (c != null && PathManager.Builder.class.isAssignableFrom(c)) {
          builderClass = (Class<? extends PathManager.Builder>) c;
        } else {
          String errMessage = "Unable to instantiate Builder from " +
              managerType + ": does not appear to implement " +
              PathManager.Builder.class.getName();
          throw new FlumeException(errMessage);
        }
      } catch (ClassNotFoundException ex) {
        logger.error("Class not found: " + managerType, ex);
        throw new FlumeException(ex);
      }
    }

    // build the builder
    PathManager.Builder builder;
    try {
      builder = builderClass.newInstance();
    } catch (InstantiationException ex) {
      String errMessage = "Cannot instantiate builder: " + managerType;
      logger.error(errMessage, ex);
      throw new FlumeException(errMessage, ex);
    } catch (IllegalAccessException ex) {
      String errMessage = "Cannot instantiate builder: " + managerType;
      logger.error(errMessage, ex);
      throw new FlumeException(errMessage, ex);
    }

    return builder.build(context);
  }
}
