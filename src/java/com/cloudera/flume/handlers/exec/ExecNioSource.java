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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
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
public class ExecNioSource extends EventSource.Base {
  // What command to run
  final String command;
  // Should we restart the script when it finishes?
  final boolean restart;
  // Time to wait if restart is true
  final int period;
  // Return line by line, or aggregate into a single pair of events?
  final boolean inAggregateMode;

  // Flags used to signal the end of an input stream
  private final AtomicBoolean errFinished = new AtomicBoolean(false);
  private final AtomicBoolean outFinished = new AtomicBoolean(false);

  private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>();

  private static Logger LOG = LoggerFactory.getLogger(ExecNioSource.class);

  public static final String A_PROC_SOURCE = "procsource";
  public static final String A_EXEC_CMD = "execcmd";

  // Input sources
  private ReadableByteChannel stdout = null;
  private ReadableByteChannel stderr = null;

  // Two threads to read from each source
  ReaderThread readOut = null, readErr = null;
  private InputStreamPipe stdinISP = null, stderrISP = null;

  private Process proc = null;

  // Used to signal that both reader and err threads have exited
  private CountDownLatch latch = new CountDownLatch(2);

  /**
   * 
   * @param command
   *          Command line to exec and get output from.
   * @param aggregate
   *          if true, return all the data from a single exec, if false, return
   *          an event per line.
   * @param restart
   *          if true, restart exec every period ms after previous exit. if
   *          false, have source return done status after execution.
   * @param period
   *          milliseconds to wait after exec exists before restarting if
   *          restart is true.
   */
  ExecNioSource(String command, boolean aggregate, boolean restart, int period) {
    this.command = command;
    this.inAggregateMode = aggregate;
    this.restart = restart;
    this.period = period;
  }

  /**
   * Create an event gathered from an exec of a program.
   */
  static Event buildExecEvent(byte[] body, String tag, String command)
      throws InterruptedException {
    Event e = new EventImpl(body);
    Attributes.setString(e, A_PROC_SOURCE, tag);
    Attributes.setString(e, A_EXEC_CMD, command);
    Attributes.setString(e, Event.A_SERVICE, "exec");
    return e;
  }

  /**
   * Makes events from the supplied byte buffer and puts them into the specified
   * BlockingQueue. If it doesn't end with \n, then compact to shift the
   * leftover bytes to be beginning of the buffer. When this function exits, the
   * buffer is in write mode, still contains any leftovers and leaves position
   * pointing to the end of the incomplete line.
   * 
   * @param buf
   *          ByteBuffer in write mode. If this method exits normally, there are
   *          no remaining '\n's in the buffer and the buf is in write mode
   * @return true if any bytes have been consumed
   */
  static boolean extractLines(ByteBuffer buf, String command, String tag,
      BlockingQueue<Event> sync) throws InterruptedException {
    buf.flip();
    boolean madeProgress = false;
    int start = buf.position();
    buf.mark();
    while (buf.hasRemaining()) {
      byte b = buf.get();
      // TODO windows: ('\r\n') line separators
      if (b == '\n') {
        int end = buf.position();
        int sz = end - start - 1; // exclude '\n'
        int maxEventSz = (int) FlumeConfiguration.get().getEventMaxSizeBytes();
        if (sz > maxEventSz) {
          // Truncating path.
          byte[] body = new byte[(int) maxEventSz];
          buf.reset(); // go back to mark
          buf.get(body, 0, maxEventSz); // read data
          buf.position(end);
          buf.mark(); // new mark.
          start = buf.position();

          sync.put(buildExecEvent(body, tag, command));
          madeProgress = true;
        } else {
          byte[] body = new byte[sz];
          buf.reset(); // go back to mark
          buf.get(body, 0, sz); // read data
          buf.get(); // read '\n'
          buf.mark(); // new mark.
          start = buf.position();

          sync.put(buildExecEvent(body, tag, command));
          madeProgress = true;
        }
      }
    }

    // rewind for any left overs
    buf.reset();
    buf.compact(); // shift leftovers to front.
    return madeProgress;
  }

  /**
   * @param in
   *          byte buffer in write mode
   * @return true if needs to stay in drop mode, false if reached a '\n' char.
   *         Buffer is in write mode when it exits.
   */
  static boolean dropUntilNewLine(ByteBuffer in) {
    in.flip();

    while (in.hasRemaining()) {
      if (in.get() == '\n') {
        in.compact(); // get rid of everything and flip back into normal mode
        return false;
      }
    }
    // wipe out the data and stay in drop mode.
    in.clear();
    return true;
  }

  /**
   * Polls an input and formats lines read as events, places them on the event
   * queue.
   */
  class ReaderThread extends Thread {
    ReadableByteChannel readChan = null;
    volatile boolean shutdown = false;
    String tag;
    AtomicBoolean signalDone;

    ReaderThread(ReadableByteChannel input, String tag, AtomicBoolean signal) {
      super("ReaderThread (" + command + "-" + tag + ")");
      Preconditions.checkArgument(input != null);
      Preconditions.checkArgument(signal != null);
      this.readChan = input;
      this.tag = tag;
      this.signalDone = signal;
    }

    /**
     * Returns true of the process is terminated
     * 
     * @param proc
     * @return true if process completed, false if was in weird state
     */
    boolean isProcDone(Process proc) {
      try {
        proc.exitValue();
        return true;
      } catch (IllegalThreadStateException e) {
        // This is gross but the only java way to figure out if the
        // subprocess is running.
        return false;
      }
    }

    /**
     * Takes exec output and converts individual lines into events.
     */
    void doLineMode() {
      // make sure we have a buffer big enough to get relevant data.
      int maxEventSize = (int) FlumeConfiguration.get().getEventMaxSizeBytes();
      int bufSize = Math.max(Short.MAX_VALUE, maxEventSize * 4);
      try {
        ByteBuffer in = ByteBuffer.allocate(bufSize);
        boolean dropMode = false; // for truncations of extremely long lines

        while (!shutdown) {
          // If interrupted, this throws an IOException
          int read = readChan.read(in);

          if (read == 0) {
            // don't burn the cpu if nothing is read.
            Clock.sleep(10);
            continue;
          }

          if (read < 0) {
            // end of input stream reached.
            if (isProcDone(proc)) {
              shutdown = true;
            }
            continue;
          }

          // At this point, I have read data.
          if (dropMode) {
            dropMode = dropUntilNewLine(in);
            if (dropMode) {
              // didn't reach new line, keep dropping
              continue;
            }
            // fall through and do extract lines
          }

          // exits with 'in' in write mode
          extractLines(in, command, tag, eventQueue);

          // the leftovers bytes ideally should always be smaller than
          // maxEventSize, and has the invariant of not having a '\n' in it.
          if (in.position() > maxEventSize) {
            // read up to max size, and then throw out the rest
            LOG.error("Entry too long, truncating: " + in.position() + " > "
                + maxEventSize + "(max event size)");

            in.flip(); // read mode.
            byte[] buf = new byte[maxEventSize];
            in.get(buf);
            eventQueue.put(buildExecEvent(buf, command, tag));

            // We can now drop the remaining data, and continue to drop
            in.clear(); // back in write mode.
            dropMode = true;
          }
        }
      } catch (InterruptedException e) {
        // interruptions are only expected in shutdown.
        if (!shutdown) {
          LOG.warn(tag + " ReaderThread received "
              + "unexpected InterruptedException", e);
        }
      } catch (BufferOverflowException b) {
        // This should never happen (buffer is bigger than max event size)
        LOG.error("Event was too large for buffer", b);
      } catch (IOException e) {
        if (!shutdown) {
          LOG.warn(tag + " ReaderThread received unexpected IOException", e);
        }
      } finally {
        try {
          readChan.close();
        } catch (IOException i) {
          LOG.warn("Failed to close input stream in ExecNioSource", i);
        }
      }
      signalDone.set(true);
      latch.countDown();
    }

    /**
     * This takes exec and reads as much as it can from a single exec and then
     * creates a single event.
     */
    void doAggregateMode() {
      // make sure we have a buffer big enough to get relevant data.
      int maxEventSize = (int) FlumeConfiguration.get().getEventMaxSizeBytes();
      try {
        // TODO evaluate allocate vs allocateDirect
        ByteBuffer in = ByteBuffer.allocate(maxEventSize);

        Event evt = null;
        boolean dropMode = false;
        while (!shutdown) {
          // If interrupted, this throws an IOException
          int read = readChan.read(in);

          if (read == 0) { // nothing read? chill out for a bit.
            Clock.sleep(100);
            continue;
          }

          if (read < 0) { // read end of input stream.
            if (isProcDone(proc)) {
              shutdown = true;
            }

            if (evt != null) {
              // full event already saved off
              eventQueue.put(evt);
              break;
            }

            if (evt == null && in.position() != 0) {
              // event that didn't fill the buffer
              byte[] eventBuf = new byte[in.position()];
              in.flip();
              in.get(eventBuf);
              evt = buildExecEvent(eventBuf, command, tag);
              eventQueue.put(evt);
              break;
            }

            // buffer of 0 size, (didn't read anything): do nothing.
            break;
          }

          // if we read data, keep reading into buffer. If the buffer is full,
          // read the data out of the buf
          if (dropMode) {
            in.clear();
            continue;
          }
          if (in.remaining() == 0) {
            byte[] eventBuf = new byte[in.position()];
            in.flip();
            in.get(eventBuf);
            evt = buildExecEvent(eventBuf, command, tag);

            in.clear(); // don't need the data any more
            dropMode = true;
          }

        }
      } catch (InterruptedException e) {
        // interruptions are expected in shutdown.
        if (!shutdown) {
          LOG.warn(tag + " ReaderThread received "
              + "unexpected InterruptedException", e);
        }
      } catch (IOException e) {
        if (!shutdown) {
          LOG.warn(tag + " ReaderThread received unexpected IOException", e);
        }
      } finally {
        try {
          readChan.close();
        } catch (IOException i) {
          LOG.warn("Failed to close input stream in ExecEventSource", i);
        }
        signalDone.set(true);
        latch.countDown();
      }
    }

    /**
     * Blocks on a line of input to be available from an input stream; formats
     * as an event and then places it on a queue.
     */
    public void run() {
      if (inAggregateMode) {
        // are stderr and stdout different events? so this can do two events?
        doAggregateMode();
      } else {
        doLineMode();
      }
    }

    void shutdown() throws IOException {
      this.shutdown = true;
      if (this.readChan != null) {
        readChan.close();
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
    Event line = null;
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
        // this is in the driver so needs to re-flag interrupted status
        LOG.warn("Exec next was interrupted " + e);
        Thread.currentThread().interrupt();
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

    // Just reading from stdout and stderr can block, so we wrap them with
    // InputStreamPipe allows them to be nonblocking.
    stdinISP = new InputStreamPipe(proc.getInputStream());
    stderrISP = new InputStreamPipe(proc.getErrorStream());
    stdout = (ReadableByteChannel) stdinISP.getChannel();
    stderr = (ReadableByteChannel) stderrISP.getChannel();

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
    public EventSource build(Context ctx, String... argv) {
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
      return new ExecNioSource(command, aggregate, restart, period);
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
      public EventSource build(Context ctx, String... argv) {
        Preconditions.checkArgument(argv.length == 2,
            "execPeriodic(\"cmdline \",period)");
        String command = argv[0];
        boolean aggregate = true;
        boolean restart = true;
        int period = Integer.parseInt(argv[1]);
        return new ExecNioSource(command, aggregate, restart, period);
      }
    };
  }

  /**
   * This builder creates a source that execs a long running program and takes
   * each line of input as the body of an event. It takes one argument, the
   * command to run. If the command exits, the exec source returns null signally
   * end of records.
   */
  public static SourceBuilder buildStream() {
    return new SourceBuilder() {
      @Override
      public EventSource build(Context ctx, String... argv) {
        Preconditions.checkArgument(argv.length == 1,
            "execStream(\"cmdline \")");
        String command = argv[0];
        boolean aggregate = false;
        boolean restart = false;
        int period = 0;
        return new ExecNioSource(command, aggregate, restart, period);
      }
    };
  }

  public static SourceBuilder builder() {
    return new Builder();
  }
}
