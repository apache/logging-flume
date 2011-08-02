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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A class that manages state transitions for files on a {@link FileSystem}.
 * </p>
 * <p>
 * Primarily this is meant to manage file renaming through various states. There
 * are four distinct states; NEW, OPEN, CLOSED, and INVALID (represented by
 * {@link State}) and are defined as follows.
 * <table>
 * <tr>
 * <th>State</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>NEW</td>
 * <td>The initial state. The file has not yet been {@link #open()}ed. The only
 * valid action is to open the file.</td>
 * </tr>
 * <tr>
 * <td>OPEN</td>
 * <td>The file is open (i.e. {@link #open()} has been called) and an
 * {@link OutputStream} is outstanding. The only valid action is to close the
 * file.</td>
 * </tr>
 * <tr>
 * <td>CLOSED</td>
 * <td>The file has been closed (i.e. {@link #close()} has been called). No
 * further action can be performed on this instance of {@link PathManager}. A
 * future improvement would be to support append here.</td>
 * </tr>
 * <tr>
 * <td>INVALID</td>
 * <td>An error has occurred and no further action is possible. Any state can
 * transition to INVALID. A future improvement would be to support recovery from
 * this state. Currently this is not possible.</td>
 * </tr>
 * </table>
 * </p>
 * <p>
 * When in the NEW state, no file exists. The user is expected to call
 * {@link #open()}. On open, the file is created with an
 * <q>open file</q> path name. This is
 * {@code baseDirectory + File.separator + fileName + PathManager.openExtension}
 * and should indicate to of the file system that this file is currently in use
 * and should be avoided (if they desire consistency). When the developer is
 * done writing data to the file's {@link OutputStream}, they should call
 * {@link #close()}. This will transition to the CLOSED state and commit the
 * file by renaming it (i.e. removing the {@link PathManager} .openExtension).
 * </p>
 * <p>
 * It is possible to understand what state the file is in by calling
 * {@link #getState()} and what the current {@link Path} is by using either
 * {@link #getOpenPath()} or {@link #getClosedPath()}, respectively.
 * </p>
 */
public class PathManager {

  private static final Logger logger = LoggerFactory
      .getLogger(PathManager.class);
  private static final String openExtension = ".tmp";

  private FileSystem fileSystem;
  private Path baseDirectory;
  private String fileName;
  private State state;

  private Path openPath;
  private Path closedPath;

  /**
   * Create a new instance that will manage {@code fileName} in the directory
   * specified by {@code baseDirectory}. The initial state is NEW.
   *
   * @param baseDirectory
   *          A directory in which we can write files.
   * @param fileName
   *          The file name local part (e.g. foo.txt).
   */
  public PathManager(FileSystem fileSystem, Path baseDirectory, String fileName) {
    this.fileSystem = fileSystem;
    this.baseDirectory = baseDirectory;
    this.fileName = fileName;
    this.state = State.NEW;

    this.openPath = new Path(baseDirectory, fileName + openExtension);
    this.closedPath = new Path(baseDirectory, fileName);
  }

  /**
   * Opens a file for write.
   *
   * @return An {@link OutputStream} for writing data.
   * @throws IOException
   */
  public synchronized OutputStream open() throws IOException {

    logger.debug("attempting to transition from " + state + " -> OPEN for "
        + this);

    switch (state) {
    case NEW:
      state = State.OPEN;

      return fileSystem.create(openPath);

    default:
      state = State.INVALID;

      throw new IllegalStateException("Illegal state transition from " + state
          + " -> OPEN");
    }
  }

  /**
   * Transition a file from open to closed, renaming it appropriately. Note that
   * this method doesn't close or flush the {@link OutputStream} returned by
   * {@link #open()}.
   *
   * @return true upon successful rename, false otherwise.
   * @throws IOException
   */
  public synchronized boolean close() throws IOException {

    logger.debug("attempting to transition from " + state + " -> CLOSED for "
        + this);

    switch (state) {
    case OPEN:
      state = State.CLOSED;

      return fileSystem.rename(openPath, closedPath);

    default:
      state = State.INVALID;

      throw new IllegalStateException("Illegal state transition from " + state
          + " -> CLOSED");
    }
  }

  @Override
  public String toString() {
    return "{ fileName:" + fileName + " state:" + state + " baseDirectory:"
        + baseDirectory + " openPath:" + openPath + " closedPath:" + closedPath
        + " fileSystem:" + fileSystem + " }";
  }

  public Path getBaseDirectory() {
    return baseDirectory;
  }

  public String getFileName() {
    return fileName;
  }

  public State getState() {
    return state;
  }

  public Path getOpenPath() {
    return openPath;
  }

  public Path getClosedPath() {
    return closedPath;
  }

  public static enum State {
    NEW, OPEN, CLOSED, INVALID,
  }

}
