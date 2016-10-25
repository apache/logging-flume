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
package org.apache.flume.conf;

import com.google.common.base.Optional;
import com.google.common.io.ByteStreams;
import org.apache.flume.Context;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

public class ExternalProcessPasswordProvider implements PasswordProvider {

  private static final String COMMAND_KEY = "command";
  private static final String CHARSET_KEY = "charset";

  private Optional<String> password = Optional.absent();

  @Override
  public String getPassword(Context context, String key) {
    if (!password.isPresent()) {
      password = Optional.of(loadPassword(context, key));
    }
    return password.get();
  }

  private String loadPassword(Context context, String key) {
    String charsetKey = key + "." + CHARSET_KEY;
    String charsetName = context.getString(charsetKey, "iso-8859-1");

    Charset charset;
    try {
      charset = Charset.forName(charsetName);
    } catch (UnsupportedCharsetException ex) {
      throw new RuntimeException("Unsupported charset: " + charsetName, ex);
    }

    String commandKey = key + "." + COMMAND_KEY;
    String command = context.getString(commandKey);
    if (command == null) {
      throw new IllegalArgumentException(commandKey + " must be set for " +
          "ExternalProcessPasswordProvider");
    }

    String commandOutput;
    try {
      commandOutput = execCommand(command, charset);
    } catch (InterruptedException | IOException ex) {
      throw new RuntimeException(ex);
    }

    return commandOutput;
  }

  private String execCommand(String command, Charset charset)
      throws IOException, InterruptedException {
    Process p = Runtime.getRuntime().exec(command);
    p.waitFor();
    if (p.exitValue() != 0) {
      String stderr;
      try {
        stderr = new String(ByteStreams.toByteArray(p.getErrorStream()), charset);
      } catch (Throwable t) {
        stderr = null;
      }
      throw new RuntimeException(
          String.format("Process (%s) exited with non-zero (%s) status code. Sterr: %s",
              command, p.exitValue(), stderr));
    }
    return new String(ByteStreams.toByteArray(p.getInputStream()), charset);
  }
}
