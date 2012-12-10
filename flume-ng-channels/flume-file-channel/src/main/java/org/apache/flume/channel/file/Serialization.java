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
package org.apache.flume.channel.file;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

class Serialization {
  private Serialization() {}

  static final long SIZE_OF_INT = 4;
  static final int SIZE_OF_LONG = 8;


  static final int VERSION_2 = 2;
  static final int VERSION_3 = 3;

  static final String METADATA_FILENAME = ".meta";
  static final String METADATA_TMP_FILENAME = ".tmp";
  static final String OLD_METADATA_FILENAME = METADATA_FILENAME + ".old";

  public static final Logger LOG = LoggerFactory.getLogger(Serialization.class);

  static File getMetaDataTempFile(File metaDataFile) {
    String metaDataFileName = metaDataFile.getName() + METADATA_TMP_FILENAME;
    return new File(metaDataFile.getParentFile(), metaDataFileName);

  }
  static File getMetaDataFile(File file) {
    String metaDataFileName = file.getName() + METADATA_FILENAME;
    return new File(file.getParentFile(), metaDataFileName);

  }

  // Support platforms that cannot do atomic renames - FLUME-1699
  static File getOldMetaDataFile(File file) {
    String oldMetaDataFileName = file.getName() + OLD_METADATA_FILENAME;
    return new File(file.getParentFile(), oldMetaDataFileName);
  }

  /**
   * Deletes all files in given directory.
   * @param checkpointDir - The directory whose files are to be deleted
   * @return - true if all files were successfully deleted, false otherwise.
   */
  static boolean deleteAllFiles(File checkpointDir) {
    if (!checkpointDir.isDirectory()) {
      return false;
    }
    StringBuilder builder = new StringBuilder("Deleted the following files from"
        + " the checkpoint directory: ");
    File[] files = checkpointDir.listFiles();
    for (File file : files) {
      if (!FileUtils.deleteQuietly(file)) {
        LOG.info(builder.toString());
        LOG.error("Error while attempting to delete: " +
            file.getName());
        return false;
      }
      builder.append(", ").append(file.getName());
    }
    builder.append(".");
    LOG.info(builder.toString());
    return true;
  }
}
