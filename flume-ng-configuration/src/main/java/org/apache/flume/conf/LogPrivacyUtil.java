/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class to help any Flume component determine whether logging potentially sensitive
 * information is allowed or not.
 * <p/>
 * InterfaceAudience.Public<br/>
 * InterfaceStability.Evolving
 */
public class LogPrivacyUtil {
  private static final Logger logger = LoggerFactory.getLogger(LogPrivacyUtil.class);
  /**
   * system property name to enable logging of potentially sensitive user data
   */
  public static final String LOG_RAWDATA_PROP = "org.apache.flume.log.rawdata";
  /**
   * system property name to enable logging of information related to the validation of agent
   * configuration at startup.
   */
  public static final String LOG_PRINTCONFIG_PROP = "org.apache.flume.log.printconfig";

  static {
    if (allowLogPrintConfig()) {
      logger.warn("Logging of configuration details of the agent has been turned on by " +
          "setting {} to true. Please use this setting with extra caution as it may result " +
          "in logging of private data. This setting is not recommended in " +
          "production environments.",
          LOG_PRINTCONFIG_PROP);
    } else {
      logger.info("Logging of configuration details is disabled. To see configuration details " +
          "in the log run the agent with -D{}=true JVM " +
          "argument. Please note that this is not recommended in production " +
          "systems as it may leak private information to the logfile.",
          LOG_PRINTCONFIG_PROP);
    }

    if (allowLogRawData()) {
      logger.warn("Logging raw data has been turned on by setting {} to true. Please use it with " +
          "extra caution as it may result in logging of potentially sensitive user data. " +
          "This setting is not recommended in production environments.",
          LOG_RAWDATA_PROP);
    }
  }

  /**
   * Tells whether logging of configuration details - including secrets - is allowed or not. This
   * is driven by a system property defined by LOG_PRINTCONFIG_PROP
   * @return true only if logging is allowed
   */
  public static boolean allowLogRawData() {
    return Boolean.getBoolean(LOG_RAWDATA_PROP);
  }

  /**
   * Tells whether logging of potentially sensitive user data is allowed or not. This
   * is driven by a system property defined by LOG_RAWDATA_PROP
   * @return true only if logging is allowed
   */
  public static boolean allowLogPrintConfig() {
    return Boolean.getBoolean(LOG_PRINTCONFIG_PROP);
  }
}
