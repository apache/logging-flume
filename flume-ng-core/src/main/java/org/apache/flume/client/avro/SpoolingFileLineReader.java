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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * A {@link LineReader} which reads log data from files stored in a
 * spooling directory and renames each file once all of its data has been
 * read (through {@link #readLine()} calls). The user must {@link #commit()}
 * each read, to indicate that the lines have been fully processed.
 * Read calls will return no data if there are no files left to read. This
 * class, in general, is not thread safe.
 *
 * This reader assumes that files with unique file names are left in the
 * spooling directory and not modified once they are placed there. Any user
 * behavior which violates these assumptions, when detected, will result in a
 * FlumeException being thrown.
 *
 * This class makes the following guarantees, if above assumptions are met:
 *   1) Once a log file has been renamed with the {@link #completedSuffix},
 *      all of its records have been read through the {@link #readLine()}
 *      function and {@link #commit()}'d exactly once.
 *   2) All files in the spooling directory will eventually be opened
 *      and delivered to a {@link #readLine()} caller.
 *
 * NOTE: This class is for internal Flume components, this is not an extension
 * point for developers customizing Flume.
 */
public class SpoolingFileLineReader implements LineReader {
  private static final Logger logger = LoggerFactory
      .getLogger(SpoolingFileLineReader.class);
  /** When a line is too long, how many characters do we print to err log. **/
  private static final int OVERFLOW_LINE_PRINT_CHARS = 30;

  private File directory;
  public String completedSuffix;
  private Optional<FileInfo> currentFile = Optional.absent();
  /** Always contains the last file from which lines have been read. **/
  private Optional<FileInfo> lastFileRead = Optional.absent();
  private boolean committed = true;
  private int bufferMaxLines;
  private int bufferMaxLineLength;
  /** A flag to signal an un-recoverable error has occured. */
  private boolean disabled = false;

  /**
   * Create a SpoolingFileLineReader to watch the given directory.
   *
   * Lines are buffered between when they are read and when they are committed.
   * The buffer has a fixed size. Its size is determined by (maxLinestoBuffer *
   * bufferSizePerLine).
   *
   * @param directory The directory to watch
   * @param completedSuffix The suffix to append to completed files
   * @param bufferMaxLines The maximum number of lines to keep in a pre-commit
   *                         buffer
   * @param bufferMaxLineLength The maximum line length for lines in the pre-commit
   *                           buffer, in characters
   */
  public SpoolingFileLineReader(File directory, String completedSuffix,
      int bufferMaxLines, int bufferMaxLineLength) {
    // Verify directory exists and is readable/writable
    Preconditions.checkNotNull(directory);
    Preconditions.checkState(directory.exists(),
        "Directory does not exist: " + directory.getAbsolutePath());
    Preconditions.checkState(directory.isDirectory(),
        "Path is not a directory: " + directory.getAbsolutePath());
    Preconditions.checkState(bufferMaxLines > 0);
    Preconditions.checkState(bufferMaxLineLength > 0);

    // Do a canary test to make sure we have access to spooling directory
    try {
      File f1 = File.createTempFile("flume", "test", directory);
      Files.write("testing flume file permissions\n", f1, Charsets.UTF_8);
      Files.readLines(f1, Charsets.UTF_8);
      f1.delete();
    } catch (IOException e) {
      throw new FlumeException("Unable to read and modify files" +
          " in the spooling directory: " + directory, e);
    }
    this.directory = directory;
    this.completedSuffix = completedSuffix;
    this.bufferMaxLines = bufferMaxLines;
    this.bufferMaxLineLength = bufferMaxLineLength;
  }

  /** Return the filename which generated the data from the last successful
   * {@link #readLines()} call. Returns null if called before any file
   * contents are read. */
  public String getLastFileRead() {
    if (!lastFileRead.isPresent()) {
      return null;
    }
    return lastFileRead.get().getFile().getAbsolutePath();
  }

  @Override
  public String readLine() throws IOException {
    if (disabled) {
      throw new IllegalStateException("Reader has been disabled.");
    }
    List<String> read = readLines(1);
    if (read.size() == 0) {
      return null;
    }
    return read.get(0);
  }

  /** Commit the last lines which were read. */
  public void commit() throws IOException {
    if (disabled) {
      throw new IllegalStateException("Reader has been disabled.");
    }
    currentFile.get().reader.mark(bufferMaxLines * bufferMaxLineLength);
    committed = true;
  }

  @Override
  /** Reads up to n lines from the current file. Returns an empty list if no
   *  files are left to read in the directory. */
  public List<String> readLines(int n) throws IOException {
    if (disabled) {
      throw new IllegalStateException("Reader has been disabled.");
    }
    if (!committed) {
      if (!currentFile.isPresent()) {
        throw new IllegalStateException("File should not roll when " +
          " commit is outstanding.");
      }
      logger.info("Last read was never comitted - resetting mark position.");
      currentFile.get().getReader().reset();
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

    String outLine = currentFile.get().getReader().readLine();

    /* It's possible that the last read took us just up to a file boundary.
     * If so, try to roll to the next file, if there is one. */
    if (outLine == null) {
      retireCurrentFile();
      currentFile = getNextFile();
      if (!currentFile.isPresent()) {
        return Collections.emptyList();
      }
      outLine = currentFile.get().getReader().readLine();
    }

    List<String> out = Lists.newArrayList();
    while (outLine != null) {
      if (outLine.length() > bufferMaxLineLength) {
        logger.error("Found line longer than " + bufferMaxLineLength +
            " characters, cannot make progress.");
        int lastCharToPrint = Math.min(OVERFLOW_LINE_PRINT_CHARS,
            outLine.length());
        logger.error("Invalid line starts with: " +
          outLine.substring(0, lastCharToPrint));
        disabled = true;
        throw new FlumeException("Encoutered line that was too long.");
      }
      out.add(outLine);
      if (out.size() == n) { break; }
      outLine = currentFile.get().getReader().readLine();
    }

    committed = false;
    lastFileRead = currentFile;
    return out;
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
    logger.info("Preparing to move file " + currPath + " to " + newPath);

    currentFile.get().reader.close();
    File newFile = new File(currPath);

    // Verify that spooling assumptions hold
    if (newFile.lastModified() != currentFile.get().getLastModified()) {
      String message = "File has been modified since being read: " + currPath;
      throw new IllegalStateException(message);
    }
    if (newFile.length() != currentFile.get().getLength()) {
      String message = "File has changed size since being read: " + currPath;
      throw new IllegalStateException(message);
    }

    // Before renaming, check whether destination file name exists
    File existing = new File(newPath);
    if (existing.exists() &&
        System.getProperty("os.name").toLowerCase().indexOf("win") >= 0) {
      /*
       * If we are here, it means the completed file already exists. In almost
       * every case this means the user is violating an assumption of Flume
       * (that log files are placed in the spooling directory with unique
       * names). However, there is a corner case on Windows systems where the
       * file was already rolled but the rename was not atomic. If that seems
       * likely, we let it pass with only a warning.
       */
      if (Files.equal(currentFile.get().getFile(), existing)) {
        logger.warn("Completed file " + newPath +
            " already exists, but files match, so continuing.");
        boolean deleted = newFile.delete();
        if (!deleted) {
          logger.error("Unable to delete file " + newFile.getAbsolutePath() +
              ". It will likely be ingested another time.");
        }
      } else {
        String message = "File name has been re-used with different" +
            " files. Spooling assumption violated for " + newPath;
        throw new IllegalStateException(message);
      }
    } else if (existing.exists()) { // Dest file exists and not on windows
      String message = "File name has been re-used with different" +
          " files. Spooling assumption violated for " + newPath;
      throw new IllegalStateException(message);
    } else  { // Dest file does not already exist
      boolean renamed = newFile.renameTo(new File(newPath));
      if (!renamed) {
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
      public boolean accept(File pathName) {
        if ((pathName.getName().endsWith(completedSuffix)) ||
            (pathName.getName().startsWith("."))) {
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
        int bufferSize = bufferMaxLines * bufferMaxLineLength;
        BufferedReader reader = new BufferedReader(new FileReader(nextFile),
            bufferSize);
        reader.mark(bufferSize);
        return Optional.of(new FileInfo(nextFile, reader));
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

  /** An immutable class with information about a file being processed. */
  private class FileInfo {
    private File file;
    private long length;
    private long lastModified;
    private BufferedReader reader;

    public FileInfo(File file, BufferedReader reader) {
      this.file = file;
      this.length = file.length();
      this.lastModified = file.lastModified();
      this.reader = reader;
    }

    public long getLength() { return this.length; }
    public long getLastModified() { return this.lastModified; }
    public BufferedReader getReader() { return this.reader; }
    public File getFile() { return this.file; }
  }

  @Override
  public void close() throws IOException {
    // No-op
  }
}
