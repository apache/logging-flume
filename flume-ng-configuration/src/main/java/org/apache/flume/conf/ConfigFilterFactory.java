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
package org.apache.flume.conf;

import com.google.common.base.Preconditions;
import org.apache.flume.FlumeException;
import org.apache.flume.configfilter.ConfigFilter;
import org.apache.flume.conf.configfilter.ConfigFilterType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class ConfigFilterFactory {

  private static final Logger LOGGER = LoggerFactory
      .getLogger(ConfigFilterFactory.class);

  public static ConfigFilter create(String name, String type) throws FlumeException {
    Preconditions.checkNotNull(name, "name");
    Preconditions.checkNotNull(type, "type");
    LOGGER.info("Creating instance of configfilter {}, type {}", name, type);
    Class<? extends ConfigFilter> aClass = getClass(type);
    try {
      ConfigFilter configFilter = aClass.newInstance();
      configFilter.setName(name);
      return configFilter;
    } catch (Exception ex) {
      throw new FlumeException("Unable to create configfilter: " + name
          + ", type: " + type + ", class: " + aClass.getName(), ex);
    }
  }

  public static Class<? extends ConfigFilter> getClass(String type) throws FlumeException {
    String classname = type;
    ConfigFilterType srcType = ConfigFilterType.OTHER;
    try {
      srcType = ConfigFilterType.valueOf(type.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException ex) {
      LOGGER.debug("Configfilter type {} is a custom type", type);
    }
    if (srcType != ConfigFilterType.OTHER) {
      classname = srcType.getClassName();
    }
    try {
      return (Class<? extends ConfigFilter>) Class.forName(classname);
    } catch (Exception ex) {
      throw new FlumeException("Unable to load configfilter type: " + type
          + ", class: " + classname, ex);
    }
  }
}
