/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.tools;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/*
 * This class provides version info of Flume NG
 */

public class VersionInfo {

  private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss z yyyy");
  private static final Properties properties = new Properties();

  static {
    try (InputStream is = VersionInfo.class.getResourceAsStream("VersionInfo.properties");) {
      properties.load(is);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Get the Flume version.
   *
   * @return the Flume version string, eg. "1.1"
   */
  public static String getVersion() {
    return properties.getProperty("version", "Unknown");
  }

  /**
   * Get the subversion revision number for the root directory
   *
   * @return the revision number, eg. "100755"
   */
  public static String getRevision() {
    return properties.getProperty("revision", "Unknown");
  }

  /**
   * Get the branch on which this originated.
   *
   * @return The branch name, e.g. "trunk" or "branches/branch-1.1"
   */
  public static String getBranch() {
    return properties.getProperty("branch", "Unknown");
  }

  /**
   * The date that Flume was compiled.
   *
   * @return the compilation date in unix date format
   */
  public static String getDate() {
    String timestamp = properties.getProperty("date");
    return timestamp == null
        ? "Unknown"
        : Instant.parse(timestamp).atZone(ZoneOffset.UTC).format(formatter);
  }

  /**
   * The user that compiled Flume.
   *
   * @return the username of the user
   */
  public static String getUser() {
    return properties.getProperty("user", "Unknown");
  }

  /**
   * Get the subversion URL for the root Flume directory.
   */
  public static String getUrl() {
    return properties.getProperty("url", "Unknown");
  }

  /**
   * Get the checksum of the source files from which Flume was
   * built.
   **/
  public static String getSrcChecksum() {
    return properties.getProperty("srcChecksum", "Unknown");
  }

  /**
   * Returns the build version info which includes version,
   * revision, user, date and source checksum
   */
  public static String getBuildVersion() {
    return VersionInfo.getVersion() +
        " from " + VersionInfo.getRevision() +
        " by " + VersionInfo.getUser() +
        " on " + VersionInfo.getDate() +
        " source checksum " + VersionInfo.getSrcChecksum();
  }

  public static void main(String[] args) {
    System.out.println("Flume " + getVersion());
    System.out.println("Source code repository: "
            + "https://git.apache.org/repos/asf/flume.git");
    System.out.println("Revision: " + getRevision());
    System.out.println("Compiled by " + getUser() + " on " + getDate());
    System.out.println("From source with checksum " + getSrcChecksum());
  }

}
