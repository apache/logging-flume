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
package org.apache.flume.tools;

import com.google.common.base.Preconditions;

import java.util.Arrays;

public class FlumeToolsMain implements FlumeTool {

  // Does the actual work in the run method so we can test it without actually
  // having to start another process.

  public static void main(String[] args) throws Exception {
    new FlumeToolsMain().run(args);
  }

  private FlumeToolsMain() {
    //No op.
  }

  @Override
  public void run(String[] args) throws Exception{
    String error = "Expected name of tool and arguments for" +
      " tool to be passed in on the command line. Please pass one of the " +
      "following as arguments to this command: \n";
    StringBuilder builder = new StringBuilder(error);
    for(FlumeToolType type : FlumeToolType.values()) {
      builder.append(type.name()).append("\n");
    }
    if(args == null || args.length == 0) {
      System.out.println(builder.toString());
      System.exit(1);
    }
    String toolName = args[0];
    FlumeTool tool = null;
    for(FlumeToolType type : FlumeToolType.values()) {
      if(toolName.equalsIgnoreCase(type.name())) {
        tool = type.getClassInstance().newInstance();
        break;
      }
    }
    Preconditions.checkNotNull(tool, "Cannot find tool matching " + toolName
      + ". Please select one of: \n " + FlumeToolType.getNames());
    if (args.length == 1) {
      tool.run(new String[0]);
    } else {
      tool.run(Arrays.asList(args).subList(1, args.length).
        toArray(new String[0]));
    }
  }
}
