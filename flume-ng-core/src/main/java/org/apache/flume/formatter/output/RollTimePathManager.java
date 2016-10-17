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

import org.apache.flume.Context;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;

/**
 *
 */
public class RollTimePathManager extends DefaultPathManager {

  private final DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMddHHmmss");
  private String lastRoll;

  public RollTimePathManager(Context context) {
    super(context);
  }

  @Override
  public File nextFile() {
    StringBuilder sb = new StringBuilder();
    String date = formatter.print(LocalDateTime.now());
    if (!date.equals(lastRoll)) {
      getFileIndex().set(0);
      lastRoll = date;
    }
    sb.append(getPrefix()).append(date).append("-");
    sb.append(getFileIndex().incrementAndGet());
    if (getExtension().length() > 0) {
      sb.append(".").append(getExtension());
    }
    currentFile = new File(getBaseDirectory(), sb.toString());

    return currentFile;
  }

  public static class Builder implements PathManager.Builder {
    @Override
    public PathManager build(Context context) {
      return new RollTimePathManager(context);
    }
  }

}
