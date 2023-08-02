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
package org.apache.flume.configfilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.Map;
import java.util.Scanner;

public class ExternalProcessConfigFilter extends AbstractConfigFilter {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalProcessConfigFilter.class);

  private static final String COMMAND_KEY = "command";
  private static final String CHARSET_KEY = "charset";
  private static final String CHARSET_DEFAULT = "UTF-8";

  Charset charset;
  private String command;

  @Override
  public String filter(String key) {
    try {
      return execCommand(key);
    } catch (InterruptedException | IllegalStateException | IOException ex) {
      LOGGER.error("Error while reading value for key {}: ", key, ex);
    }
    return null;
  }

  @Override
  public void initializeWithConfiguration(Map<String, String> configuration) {
    String charsetName = configuration.getOrDefault(CHARSET_KEY, CHARSET_DEFAULT);
    try {
      charset = Charset.forName(charsetName);
    } catch (UnsupportedCharsetException ex) {
      throw new RuntimeException("Unsupported charset: " + charsetName, ex);
    }

    command = configuration.get(COMMAND_KEY);
    if (command == null) {
      throw new IllegalArgumentException(COMMAND_KEY + " must be set for " +
          "ExternalProcessConfigFilter");
    }

  }

  private String execCommand(String key) throws IOException, InterruptedException {
    String[] split = command.split("\\s+");
    int newLength = split.length + 1;
    String[] commandParts = Arrays.copyOf(split, newLength);
    commandParts[newLength - 1] = key;
    Process p = Runtime.getRuntime().exec(commandParts);
    p.waitFor();
    if (p.exitValue() != 0) {
      String stderr;
      try {
        stderr = getResultFromStream(p.getErrorStream());
      } catch (Throwable t) {
        stderr = null;
      }
      throw new IllegalStateException(
          String.format("Process (%s) exited with non-zero (%s) status code. Sterr: %s",
              this.command, p.exitValue(), stderr));
    }


    return getResultFromStream(p.getInputStream());
  }

  private String getResultFromStream(InputStream inputStream) {
    try (Scanner scanner = new Scanner(inputStream, charset.name())) {
      String result = null;
      if (scanner.hasNextLine()) {
        result = scanner.nextLine();
        if (scanner.hasNextLine()) {
          LOGGER.warn("External process has more than one line of output. " +
              "Only the first line is used.");
        }
      } else {
        LOGGER.warn("External process has not produced any output.");
      }

      return result;
    }
  }
}
