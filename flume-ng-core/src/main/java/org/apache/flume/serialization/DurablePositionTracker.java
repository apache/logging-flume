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

package org.apache.flume.serialization;

import java.io.File;
import java.io.IOException;

import com.google.common.base.Preconditions;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.tools.PlatformDetect;

/**
 * <p/>Class that stores object state in an avro container file.
 * The file is only ever appended to.
 * At construction time, the object reads data from the end of the file and
 * caches that data for use by a client application. After construction, reads
 * never go to disk.
 * Writes always flush to disk.
 *
 * <p/>Note: This class is not thread-safe.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DurablePositionTracker implements PositionTracker {

  private final File trackerFile;
  private final DataFileWriter<TransferStateFileMeta> writer;
  private final DataFileReader<TransferStateFileMeta> reader;
  private final TransferStateFileMeta metaCache;

  private String target;

  private boolean isOpen;

  /**
   * If the file exists at startup, then read it, roll it, and open a new one.
   * We go through this to avoid issues with partial reads at the end of the
   * file from a previous crash. If we append to a bad record,
   * our writes may never be visible.
   * @param trackerFile
   * @param target
   * @return
   * @throws IOException
   */
  public static DurablePositionTracker getInstance(File trackerFile,
      String target) throws IOException {

    if (!trackerFile.exists()) {
      return new DurablePositionTracker(trackerFile, target);
    }

    // exists
    DurablePositionTracker oldTracker =
        new DurablePositionTracker(trackerFile, target);
    String existingTarget = oldTracker.getTarget();
    long targetPosition = oldTracker.getPosition();
    oldTracker.close();

    File tmpMeta = File.createTempFile(trackerFile.getName(), ".tmp",
        trackerFile.getParentFile());
    tmpMeta.delete();
    DurablePositionTracker tmpTracker =
        new DurablePositionTracker(tmpMeta, existingTarget);
    tmpTracker.storePosition(targetPosition);
    tmpTracker.close();

    // On windows, things get messy with renames...
    // FIXME: This is not atomic. Consider implementing a recovery procedure
    // so that if it does not exist at startup, check for a rolled version
    // before creating a new file from scratch.
    if (PlatformDetect.isWindows()) {
      if (!trackerFile.delete()) {
        throw new IOException("Unable to delete existing meta file " +
            trackerFile);
      }
    }

    // rename tmp file to meta
    if (!tmpMeta.renameTo(trackerFile)) {
      throw new IOException("Unable to rename " + tmpMeta + " to " +
          trackerFile);
    }

    // return a new known-good version that is open for append
    DurablePositionTracker newTracker =
        new DurablePositionTracker(trackerFile, existingTarget);
    return newTracker;
  }


  /**
   * If the file exists, read it and open it for append.
   * @param trackerFile
   * @param target
   * @throws IOException
   */
  DurablePositionTracker(File trackerFile, String target)
      throws IOException {

    Preconditions.checkNotNull(trackerFile, "trackerFile must not be null");
    Preconditions.checkNotNull(target, "target must not be null");

    this.trackerFile = trackerFile;
    this.target = target;

    DatumWriter<TransferStateFileMeta> dout =
        new SpecificDatumWriter<TransferStateFileMeta>(
            TransferStateFileMeta.SCHEMA$);

    DatumReader<TransferStateFileMeta> din =
        new SpecificDatumReader<TransferStateFileMeta>(
            TransferStateFileMeta.SCHEMA$);

    writer = new DataFileWriter<TransferStateFileMeta>(dout);

    if (trackerFile.exists()) {
      // open it for append
      writer.appendTo(trackerFile);

      reader = new DataFileReader<TransferStateFileMeta>(trackerFile, din);
      this.target = reader.getMetaString("file");
    } else {
      // create the file
      this.target = target;
      writer.setMeta("file", target);
      writer.create(TransferStateFileMeta.SCHEMA$, trackerFile);
      reader = new DataFileReader<TransferStateFileMeta>(trackerFile, din);
    }

    target = getTarget();

    // initialize @ line = 0;
    metaCache = TransferStateFileMeta.newBuilder().setOffset(0L).build();

    initReader();

    isOpen = true;
  }

  /**
   * Read the last record in the file.
   */
  private void initReader() throws IOException {
    long syncPos = trackerFile.length() - 256L;
    if (syncPos < 0) syncPos = 0L;
    reader.sync(syncPos);
    while (reader.hasNext()) {
      reader.next(metaCache);
    }
  }

  @Override
  public synchronized void storePosition(long position) throws IOException {
    metaCache.setOffset(position);
    writer.append(metaCache);
    writer.sync();
    writer.flush();
  }

  @Override
  public synchronized long getPosition() {
    return metaCache.getOffset();
  }

  @Override
  public String getTarget() {
    return target;
  }

  @Override
  public void close() throws IOException {
    if (isOpen) {
      writer.close();
      reader.close();
      isOpen = false;
    }
  }

}