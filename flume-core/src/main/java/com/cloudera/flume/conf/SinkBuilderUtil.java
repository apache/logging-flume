/**
 * Copyright 2011 The Apache Software Foundation
 *
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
 */package com.cloudera.flume.conf;

import org.antlr.runtime.RecognitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeBuilder.FunctionSpec;
import com.cloudera.flume.handlers.text.FormatFactory;
import com.cloudera.flume.handlers.text.output.OutputFormat;
import com.google.common.base.Preconditions;

/**
 * This class contains helpers for sink builders and source builders to use.
 * This is where code such as context resolution can be handled. 
 */
public class SinkBuilderUtil {
  private static Logger LOG = LoggerFactory.getLogger(SinkBuilderUtil.class);

  /**
   * This converts and creates an instance of an OutputFormat. It expects either
   * 1)a default from the flume-site.xml file, which can be overridden by 2) a
   * FunctionSpec called "format" in the context, 3) an outputformat
   * FunctionSpec argument, or 4) a string argument that it will convert to a
   * function spec but warn but convert a deprecated string argument
   *
   * @param ctx
   * @param arg
   * @return
   * @throws FlumeSpecException
   */
  public static OutputFormat resolveOutputFormat(Context ctx, Object arg)
      throws FlumeSpecException {
    // If an argument is specified, use it with highest precedence.
    if (arg != null) {
      // this will warn about deprecation if it a string instead of outputformat
      // spec
      return FlumeBuilder.createFormat(FormatFactory.get(), arg);
    }

    // Next try the context. This must be a output format FunctionSpec.
    Object format = ctx.getObj("format", Object.class);
    if (format != null) {
      return FlumeBuilder.createFormat(FormatFactory.get(), format);
    }

    // Last attempt, load from xml config file, and parse it. Ideally the
    // FlumeConfiguration settings would be in the context, but we don't support
    // this yet.
    String strFormat = FlumeConfiguration.get().getDefaultOutputFormat();
    try {
      format = FlumeBuilder.buildSimpleArg(FlumeBuilder.parseArg(strFormat));
    } catch (RecognitionException e) {
      throw new FlumeSpecException("Unable to parse output format: "
          + strFormat);
    }
    return FlumeBuilder.createFormat(FormatFactory.get(), format);
  }
}
