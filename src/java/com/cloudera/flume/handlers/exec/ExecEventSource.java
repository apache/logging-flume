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

package com.cloudera.flume.handlers.exec;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SourceFactory.SourceBuilder;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSource;
import com.cloudera.util.Clock;
import com.cloudera.util.InputStreamPipe;
import com.google.common.base.Preconditions;

/**
 * Simple process output source. Uses threads to asynchronously read stdout and
 * stderr in order to ensure that system buffers are drained.
 * 
 * Events are either returned line-by-line or aggregated into a single event
 * containing an entire process' output.
 * 
 * TODO(henry) - expose more of 'exec' parameters to callers, like ENV and CWD
 * setting.
 */
public class ExecEventSource extends EventSource.Base {
  // Input sources
  ReadableByteChannel stdout = null;
  ReadableByteChannel stderr = null;

  // Two threads to read from each source
  ReaderThread readOut = null, readErr = null;

  // What command to run
  String command = null;
  Process proc = null;

  // Should we restart the script when it finishes?
  boolean restart = false;
  // Time to wait if restart is true
  int period = 0;
  // Return line by line, or aggregate into a single pair of events?
  boolean aggregate = false;

  // Flags used to signal the end of an input stream
  final AtomicBoolean errFinished = new AtomicBoolean(false);
  final AtomicBoolean outFinished = new AtomicBoolean(false);

  final BlockingQueue<EventImpl> eventQueue = new LinkedBlockingQueue<EventImpl>();

  static final Logger LOG = LoggerFactory.getLogger(ExecEventSource.class);

  public static final String A_PROC_SOURCE = "procsource";
  public static final String A_EXEC_CMD = "execcmd";
  InputStreamPipe stdinISP = null, stderrISP = null;

  // Used to signal that both reader and err threads have exited
  CountDownLatch latch = new CountDownLatch(2);

  ExecEventSource(String command, boolean aggregate, boolean restart, int period) {
    this.command = command;
    this.aggregate = aggregate;
    this.restart = restart;
    this.period = period;
  }

  /**
   * Polls an input and formats lines read as events, places them on the event
   * queue.
   */
  class ReaderThread extends Thread {
    ReadableByteChannel input = null;
    volatile boolean shutdown = false;
    String tag;
    AtomicBoolean signalDone;
    List<ByteBuffer> buffers = new LinkedList<ByteBuffer>();

    ReaderThread(ReadableByteChannel input, String tag, AtomicBoolean signal) {
      super("ReaderThread (" + command + "-" + tag + ")");
      Preconditions.checkArgument(input != null);
      Preconditions.checkArgument(signal != null);
      this.input = input;
      this.tag = tag;
      this.signalDone = signal;
    }

    /**
     * Blocks on a line of input to be available from an input stream; formats
     * as an event and then places it on a queue.
     */
    public void run() {
      int maxEventSize = (int) FlumeConfiguration.get().getEventMaxSizeBytes();
      // Aggregate events are copied twice,
      // individual events are copied three times (because they are split
      // from the original buffer)
      try {
        ByteBuffer in = ByteBuffer.allocate(32);
        ByteBuffer buf = ByteBuffer.allocate(maxEventSize);
        while (!shutdown) {
          in.clear();
          // If interrupted, this throws an IOException
          int read = input.read(in);
          if (read == 0) {
            // don't burn cpu if nothing is read.
            Clock.sleep(100);
            continue;
          }
          if (read != -1) {
            if (!aggregate) {
              // Search for a '\n'
              in.rewind();
              int lastFound = -1;
              for (int i = 0; i < read; ++i) {
                if (in.array()[i] == (byte) '\n') {
                  // Take a shallow copy of the buffer
                  ByteBuffer prefix = in.slice();
                  // Contract the copy to a single line of input
                  prefix.limit(i);
                  prefix.position(lastFound + 1);
                  // Copy to the output
                  buf.put(prefix);
                  // Reset the position of the buffer to 0 and the limit to the
                  // the end of the last write
                  buf.flip();
                  // Offer as an event
                  ByteBuffer b = ByteBuffer.allocate(buf.limit());
                  b.put(buf);
                  EventImpl e = new EventImpl(b.array());
                  Attributes.setString(e, A_PROC_SOURCE, tag);
                  Attributes.setString(e, A_EXEC_CMD, command);
                  Attributes.setString(e, Event.A_SERVICE, "exec");
                  eventQueue.put(e);

                  // Empty out the event buffer
                  buf.clear();
                  lastFound = i;
                }
              }
              // After we have added all the '\n', we must fill the outgoing
              // buffer with what's remaining
              if (read != 0) {
                in.position(lastFound + 1);
                buf.put(in);
              }
            } else {
              if (read != 0) {
                buffers.add(in);
                in = ByteBuffer.allocate(32);
              }
            }
          } else {
            shutdown = true;
          }
        }
      } catch (InterruptedException e) {
        if (!shutdown) {
          LOG.warn(tag + " ReaderThread received "
              + "unexpected InterruptedException", e);
        }
      } catch (BufferOverflowException b) {
        // TODO: offer one full buffer?
        LOG.warn("Event was too large for buffer", b);
      } catch (IOException e) {
        if (!shutdown) {
          LOG.warn(tag + " ReaderThread received unexpected IOException", e);
        }
      } finally {
        // Make sure we offer as much as we can of the aggregate event - even
        // if there was an exception
        if (aggregate && buffers.size() > 0) {
          int total = 0;
          for (ByteBuffer b : buffers) {
            total += b.position();
          }
          ByteBuffer eventBuf = ByteBuffer.allocate(total);
          for (ByteBuffer b : buffers) {
            b.flip();
            eventBuf.put(b);
          }
          buffers.clear();
          EventImpl e = new EventImpl(eventBuf.array());
          Attributes.setString(e, A_PROC_SOURCE, tag);
          Attributes.setString(e, A_EXEC_CMD, command);
          Attributes.setString(e, Event.A_SERVICE, "exec");
          try {
            eventQueue.put(e);
          } catch (InterruptedException i) {
            LOG.warn("Unable to append exec event to queue due "
                + "to InterruptedException", i);
          }
        }
        try {
          input.close();
        } catch (IOException i) {
          LOG.warn("Failed to close input stream in ExecEventSource", i);
        }
        signalDone.set(true);
        latch.countDown();
      }
    }

    void shutdown() {
      this.shutdown = true;
      if (this.input != null) {
        this.interrupt();
      }
    }
  }

  public void close() throws IOException {
    // Note that this does not guarantee that any further next() calls will
    // return the EOF null that signals the process shut down.

    readOut.shutdown();
    readErr.shutdown();
    boolean latched = false;
    // Want to make sure that both threads have exited before we kill the
    // process
    try {
      latched = latch.await(5000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.debug("Waiting for exec thread exit was interrupted", e);
    }

    stdinISP.shutdown();
    stderrISP.shutdown();
    if (proc != null) {
      proc.destroy();
      proc = null;
    }

    if (!latched) {
      throw new IOException("Timeout waiting for exec threads to exit");
    }
  }

  /**
   * Blocks on either output from stdout / stderr or process exit (at which
   * point it throws an exception)
   * 
   * @return an Event with two tags: the stream which produced the line
   */
  public Event next() throws IOException {
    EventImpl line = null;
    while (true) {
      try {
        line = eventQueue.poll(1000, TimeUnit.MILLISECONDS);
        if (line == null) {
          if (errFinished.get() && outFinished.get()) {
            // We may have missed events between waking up and testing
            line = eventQueue.poll();
            if (line != null) {
              updateEventProcessingStats(line);
              return line;
            }
            if (restart) {
              close();
              Thread.sleep(period);
              open();
            } else {
              return null;
            }
          }
        } else {
          updateEventProcessingStats(line);
          return line;
        }
      } catch (InterruptedException e) {
        throw new RuntimeException("ExecEventSource was interrupted - " + e);
      }
    }
  }

  /**
   * Starts a Process and two threads to read from stdout / stderr
   */
  public void open() throws IOException {
    if (proc != null) {
      throw new IllegalStateException("Tried to open exec process twice");
    }
    latch = new CountDownLatch(2);
    outFinished.set(false);
    errFinished.set(false);
    proc = Runtime.getRuntime().exec(command);

    try {
      stdinISP = new InputStreamPipe(proc.getInputStream());
      stderrISP = new InputStreamPipe(proc.getErrorStream());
      stdout = (ReadableByteChannel) stdinISP.getChannel();
      stderr = (ReadableByteChannel) stderrISP.getChannel();
    } catch (IOException e) {
      proc.getInputStream().close();
      proc.getErrorStream().close();
      proc.destroy();
      proc = null;
      throw e;
    }
    readOut = new ReaderThread(stdout, "STDOUT", outFinished);
    readErr = new ReaderThread(stderr, "STDERR", errFinished);
    stdinISP.start();
    stderrISP.start();
    readOut.start();
    readErr.start();
  }

  protected static class Builder extends SourceBuilder {
    /**
     * Takes 1-4 arguments - the command to run, whether to aggregate each
     * output as a single event, whether to restart after one execution is
     * finished, and how often if so to restart.
     */
    @Override
    public EventSource build(String... argv) {
      Preconditions.checkArgument(argv.length >= 1 && argv.length <= 4,
          "exec(\"cmdline \"[,aggregate [,restart [,period]]]], )");
      String command = argv[0];
      boolean aggregate = false;
      boolean restart = false;
      int period = 0;
      if (argv.length >= 2) {
        aggregate = Boolean.parseBoolean(argv[1]);
      }
      if (argv.length >= 3) {
        restart = Boolean.parseBoolean(argv[2]);
      }
      if (argv.length >= 4) {
        period = Integer.parseInt(argv[3]);
      }
      return new ExecEventSource(command, aggregate, restart, period);
    }
  }

  /**
   * This builder creates a source that periodically execs a program and takes
   * the entire output as the body of a event. It takes two arguments - the
   * command to run, and a time period to sleep in millis before executing
   * again.
   */
  public static SourceBuilder buildPeriodic() {
    return new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        Preconditions.checkArgument(argv.length == 2,
            "execPeriodic(\"cmdline \",period)");
        String command = argv[0];
        boolean aggregate = true;
        boolean restart = true;
        int period = Integer.parseInt(argv[1]);
        return new ExecEventSource(command, aggregate, restart, period);
      }
    };
  }

  /**
   * This builder creates a source that execs a long running program and takes
   * each line of input as the body of an event. It takes one arguemnt, the
   * command to run. If the command exits, the exec source returns null signally
   * end of records.
   */
  public static SourceBuilder buildStream() {
    return new SourceBuilder() {
      @Override
      public EventSource build(String... argv) {
        Preconditions.checkArgument(argv.length == 1,
            "execStream(\"cmdline \")");
        String command = argv[0];
        boolean aggregate = false;
        boolean restart = false;
        int period = 0;
        return new ExecEventSource(command, aggregate, restart, period);
      }
    };
  }

  public static SourceBuilder builder() {
    return new Builder();
  }
}
