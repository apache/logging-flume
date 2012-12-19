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

package org.apache.flume.client.avro;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.serialization.*;
import org.apache.flume.tools.PlatformDetect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;

/**
 * <p/>A {@link ReliableEventReader} which reads log data from files stored
 * in a spooling directory and renames each file once all of its data has been
 * read (through {@link EventDeserializer#readEvent()} calls). The user must
 * {@link #commit()} each read, to indicate that the lines have been fully
 * processed.
 * <p/>Read calls will return no data if there are no files left to read. This
 * class, in general, is not thread safe.
 *
 * <p/>This reader assumes that files with unique file names are left in the
 * spooling directory and not modified once they are placed there. Any user
 * behavior which violates these assumptions, when detected, will result in a
 * FlumeException being thrown.
 *
 * <p/>This class makes the following guarantees, if above assumptions are met:
 * <ul>
 * <li> Once a log file has been renamed with the {@link #completedSuffix},
 *      all of its records have been read through the
 *      {@link EventDeserializer#readEvent()} function and
 *      {@link #commit()}ed at least once.
 * <li> All files in the spooling directory will eventually be opened
 *      and delivered to a {@link #readEvents(int)} caller.
 * </ul>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ReliableSpoolingFileEventReader implements ReliableEventReader {

  private static final Logger logger = LoggerFactory
      .getLogger(ReliableSpoolingFileEventReader.class);

  static final String metaFileName = ".flumespool-main.meta";

  private final File directory;
  private final String completedSuffix;
  private final String deserializerType;
  private final Context deserializerContext;
  private final Pattern ignorePattern;
  private final File metaFile;
  private final boolean annotateFileName;
  private final String fileNameHeader;

  private Optional<FileInfo> currentFile = Optional.absent();
  /** Always contains the last file from which lines have been read. **/
  private Optional<FileInfo> lastFileRead = Optional.absent();
  private boolean committed = true;

  /**
   * Create a ReliableSpoolingFileEventReader to watch the given directory.
   */
  public ReliableSpoolingFileEventReader(File directory, String completedSuffix,
      String ignorePattern, File trackerDirectory,
      boolean annotateFileName, String fileNameHeader,
      String deserializerType, Context deserializerContext) throws IOException {

    // Sanity checks
    Preconditions.checkNotNull(directory);
    Preconditions.checkNotNull(completedSuffix);
    Preconditions.checkNotNull(ignorePattern);
    Preconditions.checkNotNull(trackerDirectory);
    Preconditions.checkNotNull(deserializerType);
    Preconditions.checkNotNull(deserializerContext);

    if (logger.isDebugEnabled()) {
      logger.debug("Initializing {} with directory={}, metaDir={}, " +
          "deserializer={}",
          new Object[] { ReliableSpoolingFileEventReader.class.getSimpleName(),
          directory, trackerDirectory, deserializerType });
    }

    // Verify directory exists and is readable/writable
    Preconditions.checkState(directory.exists(),
        "Directory does not exist: " + directory.getAbsolutePath());
    Preconditions.checkState(directory.isDirectory(),
        "Path is not a directory: " + directory.getAbsolutePath());

    // Do a canary test to make sure we have access to spooling directory
    try {
      File f1 = File.createTempFile("flume", "test", directory);
      Files.write("testing flume file permissions\n", f1, Charsets.UTF_8);
      Files.readLines(f1, Charsets.UTF_8);
      if (!f1.delete()) {
        throw new IOException("Unable to delete canary file " + f1);
      }
    } catch (IOException e) {
      throw new FlumeException("Unable to read and modify files" +
          " in the spooling directory: " + directory, e);
    }
    this.directory = directory;
    this.completedSuffix = completedSuffix;
    this.deserializerType = deserializerType;
    this.deserializerContext = deserializerContext;
    this.annotateFileName = annotateFileName;
    this.fileNameHeader = fileNameHeader;
    this.ignorePattern = Pattern.compile(ignorePattern);

    // ensure that meta directory exists
    if (!trackerDirectory.exists()) {
      if (!trackerDirectory.mkdir()) {
        throw new IOException("Unable to mkdir nonexistent meta directory " +
            trackerDirectory);
      }
    }

    // ensure that the meta directory is a directory
    if (!trackerDirectory.isDirectory()) {
      throw new IOException("Specified meta directory is not a directory" +
          trackerDirectory);
    }

    this.metaFile = new File(trackerDirectory, metaFileName);
  }

  /** Return the filename which generated the data from the last successful
   * {@link #readEvents(int)} call. Returns null if called before any file
   * contents are read. */
  public String getLastFileRead() {
    if (!lastFileRead.isPresent()) {
      return null;
    }
    return lastFileRead.get().getFile().getAbsolutePath();
  }

  // public interface
  public Event readEvent() throws IOException {
    List<Event> events = readEvents(1);
    if (!events.isEmpty()) {
      return events.get(0);
    } else {
      return null;
    }
  }

  public List<Event> readEvents(int numEvents) throws IOException {
    if (!committed) {
      if (!currentFile.isPresent()) {
        throw new IllegalStateException("File should not roll when " +
            "commit is outstanding.");
      }
      logger.info("Last read was never committed - resetting mark position.");
      currentFile.get().getDeserializer().reset();
    } else {
      // Check if new files have arrived since last call
      if (!currentFile.isPresent()) {
        currentFile = getNextFile();
      }
      // Return empty list if no new files
      if (!currentFile.isPresent()) {
        return Collections.emptyList();
      }
    }

    EventDeserializer des = currentFile.get().getDeserializer();
    List<Event> events = des.readEvents(numEvents);

    /* It's possible that the last read took us just up to a file boundary.
     * If so, try to roll to the next file, if there is one. */
    if (events.isEmpty()) {
      retireCurrentFile();
      currentFile = getNextFile();
      if (!currentFile.isPresent()) {
        return Collections.emptyList();
      }
      events = currentFile.get().getDeserializer().readEvents(numEvents);
    }

    if (annotateFileName) {
      String filename = currentFile.get().getFile().getAbsolutePath();
      for (Event event : events) {
        event.getHeaders().put(fileNameHeader, filename);
      }
    }

    committed = false;
    lastFileRead = currentFile;
    return events;
  }

  @Override
  public void close() throws IOException {
    if (currentFile.isPresent()) {
      currentFile.get().getDeserializer().close();
      currentFile = Optional.absent();
    }
  }

  /** Commit the last lines which were read. */
  @Override
  public void commit() throws IOException {
    if (!committed && currentFile.isPresent()) {
      currentFile.get().getDeserializer().mark();
      committed = true;
    }
  }

  /**
   * Closes currentFile and attempt to rename it.
   *
   * If these operations fail in a way that may cause duplicate log entries,
   * an error is logged but no exceptions are thrown. If these operations fail
   * in a way that indicates potential misuse of the spooling directory, a
   * FlumeException will be thrown.
   * @throws FlumeException if files do not conform to spooling assumptions
   */
  private void retireCurrentFile() throws IOException {
    Preconditions.checkState(currentFile.isPresent());

    String currPath = currentFile.get().getFile().getAbsolutePath();
    String newPath = currPath + completedSuffix;
    logger.info("Preparing to move file {} to {}", currPath, newPath);

    currentFile.get().getDeserializer().close();
    File fileToRoll = new File(currPath);

    // Verify that spooling assumptions hold
    if (fileToRoll.lastModified() != currentFile.get().getLastModified()) {
      String message = "File has been modified since being read: " + currPath;
      throw new IllegalStateException(message);
    }
    if (fileToRoll.length() != currentFile.get().getLength()) {
      String message = "File has changed size since being read: " + currPath;
      throw new IllegalStateException(message);
    }

    File destination = new File(newPath);

    // Before renaming, check whether destination file name exists
    if (destination.exists() && PlatformDetect.isWindows()) {
      /*
       * If we are here, it means the completed file already exists. In almost
       * every case this means the user is violating an assumption of Flume
       * (that log files are placed in the spooling directory with unique
       * names). However, there is a corner case on Windows systems where the
       * file was already rolled but the rename was not atomic. If that seems
       * likely, we let it pass with only a warning.
       */
      if (Files.equal(currentFile.get().getFile(), destination)) {
        logger.warn("Completed file " + newPath +
            " already exists, but files match, so continuing.");
        boolean deleted = fileToRoll.delete();
        if (!deleted) {
          logger.error("Unable to delete file " + fileToRoll.getAbsolutePath() +
              ". It will likely be ingested another time.");
        }
      } else {
        String message = "File name has been re-used with different" +
            " files. Spooling assumptions violated for " + newPath;
        throw new IllegalStateException(message);
      }

    // Dest file exists and not on windows
    } else if (destination.exists()) {
      String message = "File name has been re-used with different" +
          " files. Spooling assumptions violated for " + newPath;
      throw new IllegalStateException(message);

    // Destination file does not already exist. We are good to go!
    } else {
      boolean renamed = fileToRoll.renameTo(new File(newPath));
      if (renamed) {
        logger.debug("Successfully rolled file {} to {}", fileToRoll, newPath);

        // now we no longer need the meta file
        deleteMetaFile();
      } else {
        /* If we are here then the file cannot be renamed for a reason other
         * than that the destination file exists (actually, that remains
         * possible w/ small probability due to TOC-TOU conditions).*/
        String message = "Unable to move " + currPath + " to " + newPath +
            ". This will likely cause duplicate events. Please verify that " +
            "flume has sufficient permissions to perform these operations.";
        throw new FlumeException(message);
      }
    }
  }

  /**
   * Find and open the oldest file in the chosen directory. If two or more
   * files are equally old, the file name with lower lexicographical value is
   * returned. If the directory is empty, this will return an absent option.
   */
  private Optional<FileInfo> getNextFile() {
    /* Filter to exclude finished or hidden files */
    FileFilter filter = new FileFilter() {
      public boolean accept(File candidate) {
        String fileName = candidate.getName();
        if ((candidate.isDirectory()) ||
            (fileName.endsWith(completedSuffix)) ||
            (fileName.startsWith(".")) ||
            ignorePattern.matcher(fileName).matches()) {
          return false;
        }
        return true;
      }
    };
    List<File> candidateFiles = Arrays.asList(directory.listFiles(filter));
    if (candidateFiles.isEmpty()) {
      return Optional.absent();
    } else {
      Collections.sort(candidateFiles, new Comparator<File>() {
        public int compare(File a, File b) {
          int timeComparison = new Long(a.lastModified()).compareTo(
              new Long(b.lastModified()));
          if (timeComparison != 0) {
            return timeComparison;
          }
          else {
            return a.getName().compareTo(b.getName());
          }
        }
      });
      File nextFile = candidateFiles.get(0);
      try {
        // roll the meta file, if needed
        String nextPath = nextFile.getPath();
        PositionTracker tracker =
            DurablePositionTracker.getInstance(metaFile, nextPath);
        if (!tracker.getTarget().equals(nextPath)) {
          tracker.close();
          deleteMetaFile();
        }
        tracker = DurablePositionTracker.getInstance(metaFile, nextPath);

        // sanity check
        Preconditions.checkState(tracker.getTarget().equals(nextPath),
            "Tracker target %s does not equal expected filename %s",
            tracker.getTarget(), nextPath);

        ResettableInputStream in =
            new ResettableFileInputStream(nextFile, tracker);
        EventDeserializer deserializer = EventDeserializerFactory.getInstance
            (deserializerType, deserializerContext, in);

        return Optional.of(new FileInfo(nextFile, deserializer));
      } catch (FileNotFoundException e) {
        // File could have been deleted in the interim
        logger.warn("Could not find file: " + nextFile, e);
        return Optional.absent();
      } catch (IOException e) {
        logger.error("Exception opening file: " + nextFile, e);
        return Optional.absent();
      }
    }
  }

  private void deleteMetaFile() throws IOException {
    if (metaFile.exists() && !metaFile.delete()) {
      throw new IOException("Unable to delete old meta file " + metaFile);
    }
  }

  /** An immutable class with information about a file being processed. */
  private static class FileInfo {
    private final File file;
    private final long length;
    private final long lastModified;
    private final EventDeserializer deserializer;

    public FileInfo(File file, EventDeserializer deserializer) {
      this.file = file;
      this.length = file.length();
      this.lastModified = file.lastModified();
      this.deserializer = deserializer;
    }

    public long getLength() { return length; }
    public long getLastModified() { return lastModified; }
    public EventDeserializer getDeserializer() { return deserializer; }
    public File getFile() { return file; }
  }

}
