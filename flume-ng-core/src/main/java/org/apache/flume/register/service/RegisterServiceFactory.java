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
package org.apache.flume.register.service;

import java.util.Locale;

import org.apache.flume.FlumeException;
import org.apache.flume.conf.register.service.RegisterServiceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterServiceFactory {

  private static final Logger logger = LoggerFactory
      .getLogger(RegisterServiceFactory.class);

  /**
   * Get register service via name.
   */
  public static RegisterService newInstance(String type)
      throws FlumeException {
    RegisterServiceType registerServiceType = RegisterServiceType.OTHER;
    try {
      registerServiceType = RegisterServiceType.valueOf(type.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException ex) {
      String errMsg = "Unable to load channel type: " + type;
      logger.error(errMsg);
      throw new FlumeException(errMsg);
    }
    try {
      return ((Class<? extends RegisterService>)
          Class.forName(registerServiceType.getClassName())).newInstance();
    } catch (Exception ex) {
      String errMsg = "Unable to load channel type: " + type + ", class: " + type
          + ", ex:" + ex.toString();
      logger.error(errMsg);
      throw new FlumeException(errMsg);
    }
  }
}
