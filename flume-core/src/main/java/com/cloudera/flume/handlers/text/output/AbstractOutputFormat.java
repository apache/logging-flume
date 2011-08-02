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
package com.cloudera.flume.handlers.text.output;

import java.io.IOException;
import java.io.OutputStream;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.handlers.text.FormatFactory.OutputFormatBuilder;

public abstract class AbstractOutputFormat implements OutputFormat {

  public OutputFormatBuilder builder;

  @Override
  public abstract void format(OutputStream o, Event e) throws IOException;

  @Override
  public void setBuilder(OutputFormatBuilder builder) {
    this.builder = builder;
  }

  @Override
  public OutputFormatBuilder getBuilder() {
    return builder;
  }

}
