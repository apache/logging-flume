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

package org.apache.flume.formatter.output;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flume.Context;

public class DefaultPathManager implements PathManager {

  private long seriesTimestamp;
  private File baseDirectory;
  private AtomicInteger fileIndex;
  private String filePrefix;
  private String extension;

  private static final String DEFAULT_FILE_PREFIX = "";
  private static final String DEFAULT_FILE_EXTENSION = "";
  private static final String FILE_EXTENSION = "extension";
  private static final String FILE_PREFIX = "prefix";

  protected File currentFile;

  public DefaultPathManager(Context context) {
    filePrefix = context.getString(FILE_PREFIX, DEFAULT_FILE_PREFIX);
    extension = context.getString(FILE_EXTENSION, DEFAULT_FILE_EXTENSION);
    seriesTimestamp = System.currentTimeMillis();
    fileIndex = new AtomicInteger();
  }

  @Override
  public File nextFile() {
    StringBuilder sb = new StringBuilder();
    sb.append(filePrefix).append(seriesTimestamp).append("-");
    sb.append(fileIndex.incrementAndGet());
    if (extension.length() > 0) {
      sb.append(".").append(extension);
    }
    currentFile = new File(baseDirectory, sb.toString());

    return currentFile;
  }

  @Override
  public File getCurrentFile() {
    if (currentFile == null) {
      return nextFile();
    }

    return currentFile;
  }

  @Override
  public void rotate() {
    currentFile = null;
  }

  @Override
  public File getBaseDirectory() {
    return baseDirectory;
  }

  @Override
  public void setBaseDirectory(File baseDirectory) {
    this.baseDirectory = baseDirectory;
  }

  public long getSeriesTimestamp() {
    return seriesTimestamp;
  }

  public String getPrefix() {
    return filePrefix;
  }

  public String getExtension() {
    return extension;
  }

  public AtomicInteger getFileIndex() {
    return fileIndex;
  }

  public static class Builder implements PathManager.Builder {
    @Override
    public PathManager build(Context context) {
      return new DefaultPathManager(context);
    }
  }

}
