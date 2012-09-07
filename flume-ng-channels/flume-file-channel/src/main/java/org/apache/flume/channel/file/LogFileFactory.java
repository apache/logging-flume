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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
class LogFileFactory {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(LogFileFactory.class);
  private LogFileFactory() {}

  static LogFile.MetaDataWriter getMetaDataWriter(File file, int logFileID)
      throws IOException {
    RandomAccessFile logFile = null;
    try {
      File metaDataFile = Serialization.getMetaDataFile(file);
      if(metaDataFile.exists()) {
        return new LogFileV3.MetaDataWriter(file, logFileID);
      }
      logFile = new RandomAccessFile(file, "r");
      int version = logFile.readInt();
      if(Serialization.VERSION_2 == version) {
        return new LogFileV2.MetaDataWriter(file, logFileID);
      }
      throw new IOException("File " + file + " has bad version " +
          Integer.toHexString(version));
    } finally {
      if(logFile != null) {
        try {
          logFile.close();
        } catch(IOException e) {
          LOGGER.warn("Unable to close " + file, e);
        }
      }
    }
  }

  static LogFile.Writer getWriter(File file, int logFileID,
      long maxFileSize) throws IOException {
    if(!(file.exists() || file.createNewFile())) {
      throw new IOException("Cannot create " + file);
    }
    return new LogFileV3.Writer(file, logFileID, maxFileSize);
  }

  static LogFile.RandomReader getRandomReader(File file)
      throws IOException {
    RandomAccessFile logFile = new RandomAccessFile(file, "r");
    try {
      File metaDataFile = Serialization.getMetaDataFile(file);
      // either this is a rr for a just created file or
      // the metadata file exists and as such it's V3
      if(logFile.length() == 0L || metaDataFile.exists()) {
        return new LogFileV3.RandomReader(file);
      }
      int version = logFile.readInt();
      if(Serialization.VERSION_2 == version) {
        return new LogFileV2.RandomReader(file);
      }
      throw new IOException("File " + file + " has bad version " +
          Integer.toHexString(version));
    } finally {
      if(logFile != null) {
        try {
          logFile.close();
        } catch(IOException e) {
          LOGGER.warn("Unable to close " + file, e);
        }
      }
    }
  }

  static LogFile.SequentialReader getSequentialReader(File file)
      throws IOException {
    RandomAccessFile logFile = null;
    try {
      File metaDataFile = Serialization.getMetaDataFile(file);
      if(metaDataFile.exists()) {
        return new LogFileV3.SequentialReader(file);
      }
      logFile = new RandomAccessFile(file, "r");
      int version = logFile.readInt();
      if(Serialization.VERSION_2 == version) {
        return new LogFileV2.SequentialReader(file);
      }
      throw new IOException("File " + file + " has bad version " +
          Integer.toHexString(version));
    } finally {
      if(logFile != null) {
        try {
          logFile.close();
        } catch(IOException e) {
          LOGGER.warn("Unable to close " + file, e);
        }
      }
    }
  }
}
