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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;
import java.nio.channels.WritableByteChannel;

import org.apache.log4j.Logger;

public class InputStreamPipe {
  final static Logger LOG = Logger.getLogger(InputStreamPipe.class.getName());
  final InputStream input;
  final Pipe pipe;
  final CopyThread copyThread;

  public InputStreamPipe(InputStream in) throws IOException {
    this.input = in;
    pipe = Pipe.open();

    copyThread = new CopyThread(in, pipe.sink());
  }

  public InputStreamPipe() throws IOException {
    this(System.in);
  }

  public void start() {
    copyThread.start();
  }

  public void shutdown() {
    copyThread.shutdown();
  }

  public SelectableChannel getChannel() throws IOException {
    SelectableChannel channel = pipe.source();

    channel.configureBlocking(false);

    return (channel);
  }

  protected void finalize() {
    copyThread.shutdown();
  }

  // ---------------------------------------------------

  public static class CopyThread extends Thread {
    volatile boolean keepRunning = true;
    byte[] bytes = new byte[128];
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    InputStream in;
    WritableByteChannel out;

    CopyThread(InputStream in, WritableByteChannel out) {
      this.in = in;
      this.out = out;
      this.setDaemon(true);
    }

    public void shutdown() {
      keepRunning = false;
      this.interrupt();
      try {
        out.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public void run() {
      // this could be improved

      try {
        while (keepRunning) {
          int count = in.read(bytes);

          if (count < 0) {
            break;
          }

          buffer.clear().limit(count);

          out.write(buffer);
        }

        out.close();
      } catch (IOException e) {
        LOG.info("Input stream pipe closed",e);
      }
    }
  }
}
