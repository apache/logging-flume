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

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.commons.io.FileUtils;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.Set;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Serialization {
  private Serialization() {}

  static final long SIZE_OF_INT = 4;
  static final int SIZE_OF_LONG = 8;


  static final int VERSION_2 = 2;
  static final int VERSION_3 = 3;

  public static final String METADATA_FILENAME = ".meta";
  public static final String METADATA_TMP_FILENAME = ".tmp";
  public static final String OLD_METADATA_FILENAME = METADATA_FILENAME +
    ".old";

  // 64 K buffer to copy and compress files.
  private static final int FILE_BUFFER_SIZE = 64 * 1024;

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
   * @param excludes - Names of files which should not be deleted from this
   *                 directory.
   * @return - true if all files were successfully deleted, false otherwise.
   */
  static boolean deleteAllFiles(File checkpointDir,
    @Nullable Set<String> excludes) {
    if (!checkpointDir.isDirectory()) {
      return false;
    }

    File[] files = checkpointDir.listFiles();
    if(files == null) {
      return false;
    }
    StringBuilder builder;
    if (files.length == 0) {
      return true;
    } else {
      builder = new StringBuilder("Deleted the following files: ");
    }
    if(excludes == null) {
      excludes = Collections.emptySet();
    }
    for (File file : files) {
      if(excludes.contains(file.getName())) {
        LOG.info("Skipping " + file.getName() + " because it is in excludes " +
          "set");
        continue;
      }
      if (!FileUtils.deleteQuietly(file)) {
        LOG.info(builder.toString());
        LOG.error("Error while attempting to delete: " +
            file.getAbsolutePath());
        return false;
      }
      builder.append(", ").append(file.getName());
    }
    builder.append(".");
    LOG.info(builder.toString());
    return true;
  }

  /**
   * Copy a file using a 64K size buffer. This method will copy the file and
   * then fsync to disk
   * @param from File to copy - this file should exist
   * @param to Destination file - this file should not exist
   * @return true if the copy was successful
   */
  public static boolean copyFile(File from, File to) throws IOException {
    Preconditions.checkNotNull(from, "Source file is null, file copy failed.");
    Preconditions.checkNotNull(to, "Destination file is null, " +
      "file copy failed.");
    Preconditions.checkState(from.exists(), "Source file: " + from.toString() +
      " does not exist.");
    Preconditions.checkState(!to.exists(), "Destination file: "
      + to.toString() + " unexpectedly exists.");

    BufferedInputStream in = null;
    RandomAccessFile out = null; //use a RandomAccessFile for easy fsync
    try {
      in = new BufferedInputStream(new FileInputStream(from));
      out = new RandomAccessFile(to, "rw");
      byte[] buf = new byte[FILE_BUFFER_SIZE];
      int total = 0;
      while(true) {
        int read = in.read(buf);
        if (read == -1) {
          break;
        }
        out.write(buf, 0, read);
        total += read;
      }
      out.getFD().sync();
      Preconditions.checkState(total == from.length(),
        "The size of the origin file and destination file are not equal.");
      return true;
    } catch (Exception ex) {
      LOG.error("Error while attempting to copy " + from.toString() + " to "
        + to.toString() + ".", ex);
      Throwables.propagate(ex);
    } finally {
      Throwable th = null;
      try {
        if (in != null) {
          in.close();
        }
      } catch (Throwable ex) {
        LOG.error("Error while closing input file.", ex);
        th = ex;
      }
      try {
        if (out != null) {
          out.close();
        }
      } catch (IOException ex) {
        LOG.error("Error while closing output file.", ex);
        Throwables.propagate(ex);
      }
      if (th != null) {
        Throwables.propagate(th);
      }
    }
    // Should never reach here.
    throw new IOException("Copying file: " + from.toString() + " to: " + to
      .toString() + " may have failed.");
  }

  /**
   * Compress file using Snappy
   * @param uncompressed File to compress - this file should exist
   * @param compressed Compressed file - this file should not exist
   * @return true if compression was successful
   */
  public static boolean compressFile(File uncompressed, File compressed)
    throws IOException {
    Preconditions.checkNotNull(uncompressed,
      "Source file is null, compression failed.");
    Preconditions.checkNotNull(compressed,
      "Destination file is null, compression failed.");
    Preconditions.checkState(uncompressed.exists(), "Source file: " +
      uncompressed.toString() + " does not exist.");
    Preconditions.checkState(!compressed.exists(),
      "Compressed file: " + compressed.toString() + " unexpectedly " +
        "exists.");

    BufferedInputStream in = null;
    FileOutputStream out = null;
    SnappyOutputStream snappyOut = null;
    try {
      in = new BufferedInputStream(new FileInputStream(uncompressed));
      out = new FileOutputStream(compressed);
      snappyOut = new SnappyOutputStream(out);

      byte[] buf = new byte[FILE_BUFFER_SIZE];
      while(true) {
        int read = in.read(buf);
        if (read == -1) {
          break;
        }
        snappyOut.write(buf, 0, read);
      }
      out.getFD().sync();
      return true;
    } catch (Exception ex) {
      LOG.error("Error while attempting to compress " +
        uncompressed.toString() + " to " + compressed.toString()
        + ".", ex);
      Throwables.propagate(ex);
    } finally {
      Throwable th = null;
      try {
        if (in != null) {
          in.close();
        }
      } catch (Throwable ex) {
        LOG.error("Error while closing input file.", ex);
        th = ex;
      }
      try {
        if (snappyOut != null) {
          snappyOut.close();
        }
      } catch (IOException ex) {
        LOG.error("Error while closing output file.", ex);
        Throwables.propagate(ex);
      }
      if (th != null) {
        Throwables.propagate(th);
      }
    }
    // Should never reach here.
    throw new IOException("Copying file: " + uncompressed.toString()
      + " to: " + compressed.toString() + " may have failed.");
  }

  /**
   * Decompress file using Snappy
   * @param compressed File to compress - this file should exist
   * @param decompressed Compressed file - this file should not exist
   * @return true if decompression was successful
   */
  public static boolean decompressFile(File compressed, File decompressed)
    throws IOException {
    Preconditions.checkNotNull(compressed,
      "Source file is null, decompression failed.");
    Preconditions.checkNotNull(decompressed, "Destination file is " +
      "null, decompression failed.");
    Preconditions.checkState(compressed.exists(), "Source file: " +
      compressed.toString() + " does not exist.");
    Preconditions.checkState(!decompressed.exists(),
      "Decompressed file: " + decompressed.toString() +
        " unexpectedly exists.");

    BufferedInputStream in = null;
    SnappyInputStream snappyIn = null;
    FileOutputStream out = null;
    try {
      in = new BufferedInputStream(new FileInputStream(compressed));
      snappyIn = new SnappyInputStream(in);
      out = new FileOutputStream(decompressed);

      byte[] buf = new byte[FILE_BUFFER_SIZE];
      while(true) {
        int read = snappyIn.read(buf);
        if (read == -1) {
          break;
        }
        out.write(buf, 0, read);
      }
      out.getFD().sync();
      return true;
    } catch (Exception ex) {
      LOG.error("Error while attempting to compress " +
        compressed.toString() + " to " + decompressed.toString() +
        ".", ex);
      Throwables.propagate(ex);
    } finally {
      Throwable th = null;
      try {
        if (in != null) {
          in.close();
        }
      } catch (Throwable ex) {
        LOG.error("Error while closing input file.", ex);
        th = ex;
      }
      try {
        if (snappyIn != null) {
          snappyIn.close();
        }
      } catch (IOException ex) {
        LOG.error("Error while closing output file.", ex);
        Throwables.propagate(ex);
      }
      if (th != null) {
        Throwables.propagate(th);
      }
    }
    // Should never reach here.
    throw new IOException("Decompressing file: " +
      compressed.toString() + " to: " + decompressed.toString() +
      " may have failed.");
  }
}
