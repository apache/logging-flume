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
package com.cloudera.flume.agent.durability;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.core.EventUtil;
import com.cloudera.flume.core.MaskDecorator;
import com.cloudera.flume.handlers.debug.MemorySinkSource;
import com.cloudera.flume.handlers.endtoend.AckChecksumChecker;
import com.cloudera.flume.handlers.endtoend.AckChecksumInjector;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.hdfs.SeqfileEventSink;
import com.cloudera.flume.handlers.hdfs.SeqfileEventSource;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.handlers.rolling.RollTrigger;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.ReportUtil;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.util.FileUtil;
import com.google.common.base.Preconditions;

/**
 * This class is responsible for managing where all durable data gets written
 * and read from in the case where retries are needed.
 * 
 * In this implementation, data is assumed to be initially written to the
 * writing dir synchronously.
 * 
 * When there are error conditions, IllegalStateExceptions,
 * IllegalArgumentExceptions and NullPointerExceptions can be thrown.
 * 
 * Limitation: this WAL cannot be shared between logical nodes -- each logical
 * node currently needs to have its own WAL!
 */
public class NaiveFileWALManager implements WALManager {
  static final Logger LOG = LoggerFactory.getLogger(NaiveFileWALManager.class);

  // Batches lives in queues
  final private ConcurrentHashMap<String, WALData> table = new ConcurrentHashMap<String, WALData>();
  final private LinkedBlockingQueue<String> writingQ = new LinkedBlockingQueue<String>();
  final private LinkedBlockingQueue<String> loggedQ = new LinkedBlockingQueue<String>();
  final private LinkedBlockingQueue<String> sendingQ = new LinkedBlockingQueue<String>();
  final private LinkedBlockingQueue<String> sentQ = new LinkedBlockingQueue<String>();

  // default names of directories corresponding to data in different states.
  public static final String IMPORTDIR = "import"; // externally imported
  public static final String WRITINGDIR = "writing"; // currently writing
  public static final String LOGGEDDIR = "logged"; // write and ready to send
  public static final String SENDINGDIR = "sending"; // sending downstream.
  public static final String SENTDIR = "sent"; // all sent down stream.
  public static final String ERRORDIR = "error"; // exception? go here.
  public static final String DONEDIR = "done"; // sent and e2e acked.

  // Internal cumulative statistics
  private AtomicLong importedCount = new AtomicLong(0); // batches imported
  private AtomicLong writingCount = new AtomicLong(0); // batches being written.
  private AtomicLong loggedCount = new AtomicLong(0); // batches logged durably
  private AtomicLong sendingCount = new AtomicLong(0); // batches sending
  private AtomicLong sentCount = new AtomicLong(0); // batches sent.
  private AtomicLong ackedCount = new AtomicLong(0); // batches e2e acknowledged

  private AtomicLong retryCount = new AtomicLong(0); // batches retried
  private AtomicLong recoverCount = new AtomicLong(0); // batches recovered
  private AtomicLong errCount = new AtomicLong(0); // batches with errors

  private volatile boolean shuttingDown = false;

  private Object lock = new Object();

  /**
   * Simple record for keeping the state of tag.
   */
  static class WALData {
    State s;
    String tag;
    int sentCount = 0;

    WALData(String tag) {
      this.s = State.WRITING;
      this.tag = tag;
    }

    void incrementSentCount() {
      sentCount++;
    }

    int getSentCount() {
      return sentCount;
    }

    public String toString() {
      return "WALData { " + tag + " state:" + s + " sent:" + sentCount + "}";
    }

    static WALData recovered(String tag) {
      WALData data = new WALData(tag);
      data.s = State.LOGGED;
      return data;
    }
  };

  File importDir, writingDir, loggedDir, sendingDir, sentDir, doneDir,
      errorDir;

  File baseDir;

  public NaiveFileWALManager(File baseDir) {
    File writingDir = new File(baseDir, WRITINGDIR);
    File loggedDir = new File(baseDir, LOGGEDDIR);
    File xmitableDir = new File(baseDir, SENDINGDIR);
    File ackedDir = new File(baseDir, SENTDIR);
    File doneDir = new File(baseDir, DONEDIR);
    File errDir = new File(baseDir, ERRORDIR);
    Preconditions.checkNotNull(writingDir);
    Preconditions.checkNotNull(loggedDir);
    Preconditions.checkNotNull(xmitableDir);
    Preconditions.checkNotNull(ackedDir);
    Preconditions.checkNotNull(errDir);
    Preconditions.checkNotNull(doneDir);
    this.importDir = new File(baseDir, IMPORTDIR);
    this.writingDir = writingDir;
    this.loggedDir = loggedDir;
    this.sendingDir = xmitableDir;
    this.sentDir = ackedDir;
    this.doneDir = doneDir;
    this.errorDir = errDir;
    this.baseDir = baseDir;
  }

  public void open() throws IOException {
    synchronized (lock) {
      // make the dirs if they do not exist
      if (!FileUtil.makeDirs(importDir)) {
        throw new IOException("Unable to create import dir: " + importDir);
      }
      if (!FileUtil.makeDirs(writingDir)) {
        throw new IOException("Unable to create writing dir: " + writingDir);
      }
      if (!FileUtil.makeDirs(loggedDir)) {
        throw new IOException("Unable to create logged dir: " + loggedDir);
      }
      if (!FileUtil.makeDirs(sendingDir)) {
        throw new IOException("Unable to create sending dir: " + sendingDir);
      }
      if (!FileUtil.makeDirs(sentDir)) {
        throw new IOException("Unable to create import dir: " + sentDir);
      }
      if (!FileUtil.makeDirs(doneDir)) {
        throw new IOException("Unable to create writing dir: " + doneDir);
      }
      if (!FileUtil.makeDirs(errorDir)) {
        throw new IOException("Unable to create logged dir: " + errorDir);
      }

      if (shuttingDown) {
        LOG.warn("Strange, shutting down but now reopening");
      }
      shuttingDown = false;
      LOG.info("NaiveFileWALManager is now open");
    }
  }

  public Collection<String> getWritingTags() {
    return Collections.unmodifiableCollection(writingQ);
  }

  public Collection<String> getLoggedTags() {
    return Collections.unmodifiableCollection(loggedQ);
  }

  public Collection<String> getSendingTags() {
    return Collections.unmodifiableCollection(sendingQ);
  }

  public Collection<String> getSentTags() {
    return Collections.unmodifiableCollection(sentQ);
  }

  /**
   * This method signals prevents any new drains from being provided.
   * 
   * This is not a blocking close.
   */
  public void stopDrains() throws IOException {
    synchronized (lock) {
      if (shuttingDown) {
        LOG.warn("Already shutting down, but getting another shutting down notice, odd");
      }
      shuttingDown = true;
      LOG.info("NaiveFileWALManager shutting down");
    }
  }

  /**
   * This a call back that will record if a proper ack start and ack end have
   * been encountered.
   */
  static class AckFramingState implements AckListener {
    boolean started = false;
    boolean ended = false;
    boolean failed = false;
    String ackgroup = null;

    @Override
    public void start(String group) throws IOException {
      if (ackgroup != null) {
        LOG.warn("Unexpected multiple groups in same WAL file. "
            + "previous={} current={}", ackgroup, group);
        failed = true;
        return;
      }
      ackgroup = group;
      started = true;
    }

    @Override
    public void end(String group) throws IOException {
      // only happens if group is properly acked.
      if (ackgroup == null) {
        LOG.warn("Unexpected end ack tag {} before start ack tag", group);
        failed = true;
        return;
      }

      if (!ackgroup.equals(group)) {
        LOG.warn(
            "Ack tag mismatch: end ack tag '{}' does not start ack tag '{}'",
            group, ackgroup);
        failed = true;
        return;
      }
      ended = true;
    }

    @Override
    public void err(String group) throws IOException {
      // ignore; detected by lack of end call.
    }

    @Override
    public void expired(String key) throws IOException {
      // ignore; only relevent with retry attempts.
    }

    /**
     * This method returns true only if the ackChecker is properly framed.
     **/
    public boolean isFramingValid() {
      return !failed && started && ended;
    }
  }

  /**
   * This method attempts to recover a log and checks to see if it is either
   * corrupt or improperly framed (a properly framed file has an ack start and
   * an ack end events with proper checksum).
   * 
   * If corrupt, the file is moved into the error bucket.
   * 
   * If improperly framed, it attempts to reframe logs. After a new,
   * properly-framed log is generated, the original log is moved to the error
   * bucket. Without this, these log files will get stuck forever in e2e mode's
   * retry loop.
   */
  void recoverLog(final File dir, final String f) throws IOException,
      InterruptedException {
    LOG.info("Attempting to recover " + dir.getAbsolutePath() + " / " + f);
    MemorySinkSource strippedEvents = new MemorySinkSource();
    AckFramingState state = null;
    try {
      state = checkAndStripAckFraming(dir, f, strippedEvents);
    } catch (IOException e) {
      // try to restore as many as made it through
      restoreAckFramingToLoggedState(f, strippedEvents);
      moveToErrorState(dir, f);
      LOG.info("Recover moved {} from WRITING, rewritten to LOGGED"
          + " and old version moved to ERROR", f, e);
      return;
    }
    if (state.isFramingValid()) {
      // good, this is recoverable with just a move.
      File old = new File(dir, f);
      if (!old.isFile() || !old.renameTo(new File(loggedDir, f))) {
        throw new IOException("Unable to recover - couldn't rename " + old
            + " to " + loggedDir + f);
      }
      return;
    }

    // oh no, this had no ack close, let's restore them.
    LOG.info("Valid events in {} but does not have proper ack tags!", f);
    restoreAckFramingToLoggedState(f, strippedEvents);
    moveToErrorState(dir, f);
    LOG.info("Recover moved {} from WRITING, rewritten to LOGGED "
        + "and old version moved to ERROR", f);

    // no need to add to queues, once in LOGGED dir, the recover() function
    // takes over and adds them.
  }

  /**
   * ok now lets move the corrupt file to the error state.
   */
  private void moveToErrorState(final File dir, final String f)
      throws IOException {
    try {
      File old = new File(dir, f);
      if (!old.isFile() || !old.renameTo(new File(errorDir, f))) {
        throw new IOException("Unable to recover - couldn't rename " + old
            + " to " + loggedDir + f);
      }
    } catch (IOException e) {
      LOG.error("Failed to move incorrectly ack framed file {}", dir + f, e);
      throw e;
    }
  }

  /**
   * Takes stripped events and writes new log with proper framing to the LOGGED
   * state. This assumes all events in mem are stripped of ack related
   * attributes and events.
   */
  private void restoreAckFramingToLoggedState(final String f,
      MemorySinkSource mem) throws IOException, InterruptedException {
    EventSink ackfixed = new AckChecksumInjector<EventSink>(
        new SeqfileEventSink(new File(loggedDir, f).getAbsoluteFile()));
    try {
      ackfixed.open();
      EventUtil.dumpAll(mem, ackfixed);
      ackfixed.close();
    } catch (IOException e) {
      LOG.error("problem when attempting to fix corrupted WAL log {}", f, e);
      throw e;
    }
  }

  /**
   * Checks the framing of a the log file f, and strips all the ack related tags
   * from a WAL log file. Returns a state that knows if the group has been
   * properly ack framed, and all the stripped events are added to the memory
   * buffer.
   */
  private AckFramingState checkAndStripAckFraming(final File dir,
      final String f, MemorySinkSource mem) throws InterruptedException,
      IOException {
    EventSource src = new SeqfileEventSource(new File(dir, f).getAbsolutePath());
    AckFramingState state = new AckFramingState();
    // strip previous ack tagged attributes out of events before putting raw
    // events.
    EventSink mask = new MaskDecorator<EventSink>(mem,
        AckChecksumInjector.ATTR_ACK_TYPE, AckChecksumInjector.ATTR_ACK_TAG,
        AckChecksumInjector.ATTR_ACK_HASH);
    // check for and extract the ack events.
    AckChecksumChecker<EventSink> check = new AckChecksumChecker<EventSink>(
        mask, state);

    try {
      // copy all raw events into mem buffer
      src.open();
      check.open();
      EventUtil.dumpAll(src, check);
      src.close();
      check.close();
    } catch (IOException e) {
      LOG.warn("Recovered log file {}  was corrupt", f);
      throw e;
    }
    return state;
  }

  /**
   * This looks at directory structure and recovers state based on where files
   * are in the file system.
   * 
   * For a first cut, we will just get everything into the logged state and
   * restart from there. Optimizations can recover at finer grain and be more
   * performant.
   */
  public void recover() throws IOException {
    synchronized (lock) {
      // move all writing into the logged dir.
      for (String f : writingDir.list()) {
        try {
          recoverLog(writingDir, f);
        } catch (InterruptedException e) {
          LOG.error("Interupted when trying to recover WAL log {}", f, e);
          throw new IOException("Unable to recover " + writingDir + f);
        }
      }

      // move all sending into the logged dir
      for (String f : sendingDir.list()) {
        try {
          recoverLog(sendingDir, f);
        } catch (InterruptedException e) {
          LOG.error("Interupted when trying to recover WAL log {}", f, e);
          throw new IOException("Unable to recover " + sendingDir + f);
        }
      }

      // move all sent into the logged dir.
      for (String f : sentDir.list()) {
        try {
          recoverLog(sentDir, f);
        } catch (InterruptedException e) {
          LOG.error("Interupted when trying to recover WAL log {}", f, e);
          throw new IOException("Unable to recover " + sentDir + f);
        }
      }

      // add all logged to loggedQ and table
      for (String f : loggedDir.list()) {
        // File log = new File(loggedDir, f);
        WALData data = WALData.recovered(f);
        table.put(f, data);
        loggedQ.add(f);
        recoverCount.incrementAndGet();
        LOG.debug("Recover loaded {}", f);
      }

      // carry on now on your merry way.
    }
  }

  /**
   * This gets a new sink when rolling, and is called when rolling to a new
   * file.
   */
  public EventSink newAckWritingSink(final Tagger tagger, AckListener al)
      throws IOException {
    synchronized (lock) {
      File dir = getDir(State.WRITING);
      final String tag = tagger.newTag();

      EventSink bareSink = new SeqfileEventSink(
          new File(dir, tag).getAbsoluteFile());
      EventSink curSink = new AckChecksumInjector<EventSink>(bareSink,
          tag.getBytes(), al);

      writingQ.add(tag);
      WALData data = new WALData(tag);
      table.put(tag, data);
      writingCount.incrementAndGet();

      return new EventSinkDecorator<EventSink>(curSink) {
        @Override
        public void append(Event e) throws IOException, InterruptedException {
          getSink().append(e);
        }

        @Override
        public void close() throws IOException, InterruptedException {
          super.close();
          synchronized (lock) {
            if (!writingQ.contains(tag)) {
              LOG.warn("Already changed tag {} out of WRITING state", tag);
              return;
            }
            LOG.info("File lives in {}", getFile(tag));

            changeState(tag, State.WRITING, State.LOGGED);
            loggedCount.incrementAndGet();
          }
        }
      };
    }
  }

  /**
   * Returns a new sink when the roller asks for a new one.
   */
  public EventSink newWritingSink(final Tagger tagger) throws IOException {
    synchronized (lock) {
      File dir = getDir(State.WRITING);
      final String tag = tagger.newTag();
      EventSink curSink = new SeqfileEventSink(
          new File(dir, tag).getAbsoluteFile());
      writingQ.add(tag);
      WALData data = new WALData(tag);
      table.put(tag, data);

      return new EventSinkDecorator<EventSink>(curSink) {
        @Override
        public void append(Event e) throws IOException, InterruptedException {
          LOG.debug("Appending event: {}", e); // performance sensitive
          getSink().append(e);

        }

        @Override
        public void close() throws IOException, InterruptedException {
          synchronized (lock) {
            super.close();
            changeState(tag, State.WRITING, State.LOGGED);
          }
        }
      };
    }
  }

  @Override
  public RollSink getAckingSink(Context ctx, final RollTrigger t,
      final AckListener ackQueue, long checkMs) throws IOException {
    // TODO (jon) make this expressible using the DSL instead of only in
    // javacode
    return new RollSink(ctx, "ackingWal", t, checkMs) {
      @Override
      public EventSink newSink(Context ctx) throws IOException {
        return newAckWritingSink(t.getTagger(), ackQueue);
      }
    };
  }

  private LinkedBlockingQueue<String> getQueue(State state) {
    Preconditions.checkNotNull(state);
    switch (state) {
    case WRITING:
      return writingQ;
    case LOGGED:
      return loggedQ;
    case SENDING:
      return sendingQ;
    case SENT:
      return sentQ;
    case E2EACKED:
    case IMPORT:
    default:
      return null;
    }
  }

  private File getDir(State state) {
    Preconditions.checkNotNull(state);
    switch (state) {
    case IMPORT:
      return importDir;
    case WRITING:
      return writingDir;
    case LOGGED:
      return loggedDir;
    case SENDING:
      return sendingDir;
    case SENT:
      return sentDir;
    case ERROR:
      return errorDir;
    case E2EACKED:
      return doneDir;
    default:
      return null;
    }
  }

  /**
   * This needs to be called from a lock protected call site
   * 
   * @param tag
   * @return
   */
  private File getFile(String tag) {
    Preconditions.checkNotNull(tag, "Attempted to get file for empty tag");
    WALData data = table.get(tag);
    Preconditions.checkNotNull(data, "Data for tag " + tag + " was empty.");

    File dir = getDir(data.s);
    return new File(dir, tag);
  }

  /**
   * Change the state of a file after it has been successfully processed. The
   * generally move linearly from WRITING -> LOGGED -> SENDING -> SENT ->
   * E2EACKED. When a message reaches E2EACKED state, it is then deleted.
   * 
   * This can throw both IOExceptions and runtime exceptions due to
   * Preconditions failures.
   */
  void changeState(String tag, State oldState, State newState)
      throws IOException {
    synchronized (lock) {
      WALData data = table.get(tag);
      Preconditions.checkArgument(data != null, "Tag " + tag + " has no data");
      Preconditions.checkArgument(tag.equals(data.tag),
          "Data associated with tag didn't match tag " + tag);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Change " + data.s + "/" + oldState + " to " + newState
            + " : " + tag);
      }

      // null allows any previous state.
      if (oldState == null) {
        oldState = data.s;
      }

      if (oldState == State.SENT && newState == State.LOGGED) {
        data.incrementSentCount();
      }

      Preconditions.checkState(data.s == oldState, "Expected state to be "
          + oldState + " but was " + data.s);

      if (oldState == State.ERROR) {
        throw new IllegalStateException("Cannot move from error state");
      }

      /*
       * This uses java's File.rename and File.delete method. According to the
       * link below, Solaris (I assume POSIX/linux) does atomic rename but
       * Windows does not guarantee it.
       * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4017593 To be truly
       * correct, I need to check the return value (will likely fail in unix if
       * moving from one volume to another instead of just within same volume)
       */

      // move files to other directories to making state change durable.
      File orig = getFile(tag);
      File newf = new File(getDir(newState), tag);
      boolean success = orig.renameTo(newf);
      if (!success) {
        throw new IOException("Move  " + orig + " -> " + newf + "failed!");
      }

      // E2EACKED is terminal state just delete it.
      // TODO (jon) add option to keep logged files
      if (newState == State.E2EACKED) {
        LOG.debug("Deleting WAL file: {}", newf.getAbsoluteFile());
        boolean res = newf.delete();
        if (!res) {
          LOG.warn("Failed to delete complete WAL file: {}",
              newf.getAbsoluteFile());
        }
        table.remove(tag);
      }

      // is successful, update queues.
      LOG.debug("old state is {}", oldState);
      getQueue(oldState).remove(tag);
      BlockingQueue<String> q = getQueue(newState);
      if (q != null) {
        q.add(tag);
      }
      data.s = newState;
    }
  }

  /**
   * This decorator wraps sources and updates state transitions for the
   * different sets of data. It intercepts exceptions from its sources and moves
   * the batch to error state.
   */
  class StateChangeDeco extends EventSource.Base {
    final String tag;
    final EventSource src;

    public StateChangeDeco(EventSource src, String tag) {
      Preconditions.checkNotNull(src, "StateChangeDeco called with null src");
      this.src = src;
      this.tag = tag;
    }

    @Override
    public void open() throws IOException, InterruptedException {
      try {
        src.open();
      } catch (IOException ioe) {
        changeState(tag, State.SENDING, State.ERROR);
        errCount.incrementAndGet();
        throw ioe;
      }
    }

    @Override
    public void close() throws IOException, InterruptedException {
      try {
        src.close();
        changeState(tag, State.SENDING, State.SENT);
        sentCount.incrementAndGet();
      } catch (IOException ioe) {
        LOG.warn("close had a problem {}", src, ioe);
        changeState(tag, null, State.ERROR);
        throw ioe; // rethrow this
      }
    }

    @Override
    public Event next() throws IOException, InterruptedException {
      try {
        Event e1 = src.next();
        if (e1 == null)
          return null;

        // The roller used by the writing portion of the WAL tags values with a
        // rolltag attribute. This tag is not relevant downstream and may cause
        // a problem if a downstream roller tries to add its own rolltag. This
        // prevents that from being a problem.
        Event e2 = EventImpl.unselect(e1, RollSink.DEFAULT_ROLL_TAG);
        updateEventProcessingStats(e2);
        return e2;
      } catch (IOException ioe) {
        LOG.warn("next had a problem {}", src, ioe);
        changeState(tag, null, State.ERROR);
        errCount.incrementAndGet();
        throw ioe;
      }
    }

    @Override
    public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
      super.getReports(namePrefix, reports);
      src.getReports(namePrefix + getName() + ".", reports);
    }

    @Override
    public String toString() {
      return "NaiveFileWALManager (dir=" + baseDir.getAbsolutePath() + " )";
    }
  }

  /**
   * This gets a valid seqfile event source. If open fails, it just cleans that
   * file up and moves on to the next
   * 
   * Will block unless this manager has been told to close. When closed will
   * return null;
   */
  public EventSource getUnackedSource() throws IOException,
      InterruptedException {
    String sendingTag = null;
    try {
      while (sendingTag == null) {
        sendingTag = loggedQ.poll(200, TimeUnit.MILLISECONDS);
        // exit condition is when closed is flagged and the queue is empty.

        if (sendingTag == null) {
          synchronized (lock) {
            if (shuttingDown && loggedQ.isEmpty() && sendingQ.isEmpty())
              return null;
          }
        }
      }
    } catch (InterruptedException e) {
      LOG.error("interrupted", e);
      synchronized (lock) {
        if (!shuttingDown) {
          LOG.warn("!!! Caught interrupted exception but not closed so rethrowing interrupted. loggedQ:"
              + loggedQ.size() + " sendingQ:" + sendingQ.size());
          throw e;
        }
        if (shuttingDown) {
          if (loggedQ.isEmpty() && sendingQ.isEmpty()) {
            // if empty and interrupted, return cleanly.
            return null;
          } else {
            LOG.warn("!!! Interrupted but queues still have elements so throw exception. loggedQ:"
                + loggedQ.size() + " sendingQ:" + sendingQ.size());
            throw new IOException(e);
          }
        }
      }
    }
    synchronized (lock) {
      LOG.info("opening log file {}", sendingTag);
      changeState(sendingTag, State.LOGGED, State.SENDING);
      sendingCount.incrementAndGet();
      File curFile = getFile(sendingTag);
      EventSource curSource = new SeqfileEventSource(curFile.getAbsolutePath());
      return new StateChangeDeco(curSource, sendingTag);
    }
  }

  /**
   * Convert a tag from Sent to end-to-end acked.
   */
  public void toAcked(String tag) throws IOException {
    synchronized (lock) {
      WALData wd = getWalData(tag);
      if (wd.s == State.LOGGED && wd.getSentCount() > 0) {
        changeState(tag, State.LOGGED, State.E2EACKED);
        ackedCount.incrementAndGet();

      } else if (wd.s == State.SENT) {
        changeState(tag, State.SENT, State.E2EACKED);
        ackedCount.incrementAndGet();
      }

    }
  }

  /**
   * Change something that is sent and not acknowledged to logged state so that
   * the normal mechanisms will eventually retry sending it.
   */

  public void retry(String tag) throws IOException {
    synchronized (lock) {
      // Yuck. This is like a CAS right now.
      WALData data = table.get(tag);
      if (data == null) {
        // wrong WALManager
        return;
      }
      if (data != null) {
        switch (data.s) {
        case SENDING: {
          // This is possible if a connection goes down. If we are currently
          // sending this group, we should continue trying to send (no need to
          // restarting it by demoting to LOGGED)
          LOG.info("Attempt to retry chunk '" + tag
              + "' in SENDING state.  Data is being sent so "
              + "there is no need for state transition.");
          break;
        }
        case LOGGED: {
          // This is likely the most common case where we retry events spooled
          // to
          // disk
          LOG.info("Attempt to retry chunk '" + tag
              + "'  in LOGGED state.  There is no need "
              + "for state transition.");
          break;
        }
        case SENT: {
          // This is possible if the collector goes down or if endpoint (HDFS)
          // goes down. Here we demote the chunk back to LOGGED state.
          changeState(tag, State.SENT, State.LOGGED);
          retryCount.incrementAndGet();
          break;
        }
        case E2EACKED: {
          // This is possible but very unlikely. If a group is in this state it
          // is
          // about to be deleted and thus doesn't need a state transition.
          LOG.debug("Attempt to retry chunk  '" + tag
              + "' in E2EACKED state. There is no "
              + "need to retry because data is acked.");
          break;
        }

        case ERROR: // should never happen
          LOG.info("Attempt to retry chunk '" + tag
              + "' from ERROR state.  Data in ERROR "
              + "state stays in ERROR state so no transition.");
          break;

        case IMPORT: // should never happen
        case WRITING: // should never happen
        default: {
          String msg = "Attempt to retry from a state " + data.s
              + " which is a state do not ever retry from.";
          LOG.error(msg);
          throw new IllegalStateException(msg);
        }
        }
      }
    }
  }

  @Override
  public EventSource getEventSource() throws IOException {
    return new WALSource(this);
  }

  /**
   * This is a hook that imports external files to the WAL bypassing the default
   * append
   */
  public void importData() throws IOException {
    // move all writing into the logged dir.
    for (String fn : importDir.list()) {

      // add to logging queue
      WALData data = WALData.recovered(fn);
      synchronized (lock) {
        table.put(fn, data);
        loggedQ.add(fn);
        importedCount.incrementAndGet();
      }
    }
  }

  @Override
  public String getName() {
    return "naiveWal";
  }

  @Override
  public ReportEvent getMetrics() {
    ReportEvent rpt = new ReportEvent(getName());

    // historical counts
    rpt.setLongMetric(A_IMPORTED, importedCount.get());
    rpt.setLongMetric(A_WRITING, writingCount.get());
    rpt.setLongMetric(A_LOGGED, loggedCount.get());
    rpt.setLongMetric(A_SENDING, sendingCount.get());
    rpt.setLongMetric(A_SENT, sentCount.get());
    rpt.setLongMetric(A_ACKED, ackedCount.get());
    rpt.setLongMetric(A_RETRY, retryCount.get());
    rpt.setLongMetric(A_ERROR, errCount.get());
    rpt.setLongMetric(A_RECOVERED, recoverCount.get());

    // Waiting to send
    rpt.setLongMetric(A_IN_LOGGED, loggedQ.size());

    // waiting for ack
    rpt.setLongMetric(A_IN_SENT, sentQ.size());

    rpt.setStringMetric("sentGroups", Arrays.toString(sentQ.toArray()));
    rpt.setStringMetric("loggedGroups", Arrays.toString(loggedQ.toArray()));
    return rpt;
  }

  @Override
  public Map<String, Reportable> getSubMetrics() {
    return ReportUtil.noChildren();
  }

  @Override
  public boolean isEmpty() {
    synchronized (lock) {
      return writingQ.isEmpty() && loggedQ.isEmpty() && sendingQ.isEmpty()
          && sentQ.isEmpty();
    }
  }

  // Exposed for testing
  WALData getWalData(String tag) {
    return table.get(tag);
  }

}
