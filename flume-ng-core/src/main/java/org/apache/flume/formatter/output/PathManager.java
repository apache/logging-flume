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
import org.apache.flume.Context;

/**
 * Creates the files used by the RollingFileSink.
 */
public interface PathManager {
  /**
   * {@link Context} prefix
   */
  public static String CTX_PREFIX = "pathManager.";

  File nextFile();

  File getCurrentFile();

  void rotate();

  File getBaseDirectory();

  void setBaseDirectory(File baseDirectory);

  /**
   * Knows how to construct this path manager.<br/>
   * <b>Note: Implementations MUST provide a public a no-arg constructor.</b>
   */
  public interface Builder {
    public PathManager build(Context context);
  }
}
