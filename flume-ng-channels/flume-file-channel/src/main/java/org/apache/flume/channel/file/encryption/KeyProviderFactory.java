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
package org.apache.flume.channel.file.encryption;

import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class KeyProviderFactory {
  private static final Logger logger =
      LoggerFactory.getLogger(KeyProviderFactory.class);

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static KeyProvider getInstance(String keyProviderType, Context context) {
    Preconditions.checkNotNull(keyProviderType,
        "key provider type must not be null");

    // try to find builder class in enum of known providers
    KeyProviderType type;
    try {
      type = KeyProviderType.valueOf(keyProviderType.toUpperCase());
    } catch (IllegalArgumentException e) {
      logger.debug("Not in enum, loading provider class: {}", keyProviderType);
      type = KeyProviderType.OTHER;
    }
    Class<? extends KeyProvider.Builder> providerClass =
        type.getBuilderClass();

    // handle the case where they have specified their own builder in the config
    if (providerClass == null) {
      try {
        Class c = Class.forName(keyProviderType);
        if (c != null && KeyProvider.Builder.class.isAssignableFrom(c)) {
          providerClass = (Class<? extends KeyProvider.Builder>) c;
        } else {
          String errMessage = "Unable to instantiate Builder from " +
              keyProviderType;
          logger.error(errMessage);
          throw new FlumeException(errMessage);
        }
      } catch (ClassNotFoundException ex) {
        logger.error("Class not found: " + keyProviderType, ex);
        throw new FlumeException(ex);
      }
    }

    // build the builder
    KeyProvider.Builder provider;
    try {
      provider = providerClass.newInstance();
    } catch (InstantiationException ex) {
      String errMessage = "Cannot instantiate builder: " + keyProviderType;
      logger.error(errMessage, ex);
      throw new FlumeException(errMessage, ex);
    } catch (IllegalAccessException ex) {
      String errMessage = "Cannot instantiate builder: " + keyProviderType;
      logger.error(errMessage, ex);
      throw new FlumeException(errMessage, ex);
    }
    return provider.build(context);
  }
}
