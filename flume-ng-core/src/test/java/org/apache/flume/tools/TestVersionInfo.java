/**
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

package org.apache.flume.tools;

import static org.junit.Assert.*;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestVersionInfo {

  private static final Logger logger = LoggerFactory
      .getLogger(TestVersionInfo.class);

  /**
   *  Make sure that Unknown is expected when no version info
   */
  @Test
  public void testVersionInfoUnknown() {

    logger.debug("Flume " + VersionInfo.getVersion());
    logger.debug("Subversion " + VersionInfo.getUrl() + " -r " + VersionInfo.getRevision());
    logger.debug("Compiled by " + VersionInfo.getUser() + " on " + VersionInfo.getDate());
    logger.debug("From source with checksum " + VersionInfo.getSrcChecksum());
    logger.debug("Flume " + VersionInfo.getBuildVersion());

    assertTrue("getVersion returned Unknown",
        !VersionInfo.getVersion().equals("Unknown"));
    assertTrue("getUser returned Unknown",
        !VersionInfo.getUser().equals("Unknown"));
    assertTrue("getUrl returned Unknown",
        !VersionInfo.getUrl().equals("Unknown"));
    assertTrue("getSrcChecksum returned Unknown",
        !VersionInfo.getSrcChecksum().equals("Unknown"));

    // check getBuildVersion() return format
    assertTrue("getBuildVersion returned unexpected format",VersionInfo.
        getBuildVersion().matches(".+from.+by.+on.+source checksum.+"));

    //"Unknown" when build without svn or git
    assertNotNull("getRevision returned null", VersionInfo.getRevision());
    assertNotNull("getBranch returned null", VersionInfo.getBranch());

  }

}
