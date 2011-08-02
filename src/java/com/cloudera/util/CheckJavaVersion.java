/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.util;

import org.apache.log4j.Logger;

/**
 * This class simply checks that the current Java version is no older than 1.6.
 */
public class CheckJavaVersion {
  final public static Logger LOG = Logger.getLogger(CheckJavaVersion.class);
  // this is recorded locally
  private final static double minJavaVersion = 1.6;

  /**
   * This method first checks if the java version extracted from the system
   * properties is in the expected format. If it is in not the right format or
   * the Java version is older than 1.6 it simply returns false, and true otherwise.
   */
  public static boolean isVersionOk() {
    double currentJavaVersion = 0;

    try {
      currentJavaVersion = Double.parseDouble(System
          .getProperty("java.version").substring(0, 3));
    } catch (NumberFormatException e) {
      LOG.error("Java Version is an invalid format");
      return false; // exit with failure
    }

    if (currentJavaVersion < minJavaVersion) {
      LOG.error("Current Java Version: " + System.getProperty("java.version"));
      LOG.error("Version 1.6 or later required");
      return false; // exit with failure
    }
    return true;
  }
}
