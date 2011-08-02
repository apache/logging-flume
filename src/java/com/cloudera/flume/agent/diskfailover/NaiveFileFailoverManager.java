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
package com.cloudera.flume.agent.diskfailover;

import java.io.File;
import java.io.IOException;
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
import com.cloudera.flume.handlers.hdfs.SeqfileEventSink;
import com.cloudera.flume.handlers.hdfs.SeqfileEventSource;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.handlers.rolling.RollTrigger;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.Reportable;
import com.cloudera.util.FileUtil;
import com.google.common.base.Preconditions;

/**
 * This class is responsible for managing where faults are detected and future
 * messages need to be made durable until it has been sent downstream on hop *
 * When there are error conditions, IllegalStateExceptions,
 * IllegalArgumentExceptions and NullPointerExceptions can be thrown.
 */
public class NaiveFileFailoverManager implements DiskFailoverManager,
    Reportable {
  static final Logger LOG = LoggerFactory
      .getLogger(NaiveFileFailoverManager.class);

  // This is the state of the node.
  final private ConcurrentHashMap<String, DFOData> table = new ConcurrentHashMap<String, DFOData>();
  final private LinkedBlockingQueue<String> writingQ = new LinkedBlockingQueue<String>();
  final private LinkedBlockingQueue<String> loggedQ = new LinkedBlockingQueue<String>();
  final private LinkedBlockingQueue<String> sendingQ = new LinkedBlockingQueue<String>();

  public static final String IMPORTDIR = "dfo_import";
  public static final String WRITINGDIR = "dfo_writing";
  public static final String LOGGEDDIR = "dfo_logged";
  public static final String ERRORDIR = "dfo_error";
  public static final String SENDINGDIR = "dfo_sending";

  // Internal statistics
  private AtomicLong writingCount = new AtomicLong(0); // # of batches current
  // being written.
  private AtomicLong loggedCount = new AtomicLong(0); // # of batches logged
  // (durably written
  // locally)
  private AtomicLong sendingCount = new AtomicLong(0); // # of messages sending
  private AtomicLong sentCount = new AtomicLong(0); // # of messages resent

  private AtomicLong importedCount = new AtomicLong(0); // # of batches imported

  private AtomicLong retryCount = new AtomicLong(0); // # of batches retried
  private AtomicLong recoverCount = new AtomicLong(0); // # batches recovered
  private AtomicLong errCount = new AtomicLong(0); // # batches with errors

  private AtomicLong writingEvtCount = new AtomicLong(0);
  private AtomicLong readEvtCount = new AtomicLong(0);

  // 'closed' is the state where no data can be inserted and no data can be
  // drained from this decorator.
  // 'closing' is the state after a close is requested where the values can
  // still be drained by the subordinate thread, but no new data can be
  // inserted. This is necessary for clean closes.
  enum ManagerState {
    INIT, OPEN, CLOSED, CLOSING
  };

  volatile ManagerState state = ManagerState.INIT;

  static class DFOData {
    State s;
    String tag;

    DFOData(String tag) {
      this.s = State.WRITING;
      this.tag = tag;
    }

    static DFOData recovered(String tag) {
      DFOData data = new DFOData(tag);
      data.s = State.LOGGED;
      return data;
    }
  };

  File importDir, writingDir, loggedDir, sendingDir, errorDir;

  public NaiveFileFailoverManager(File baseDir) {
    File writingDir = new File(baseDir, WRITINGDIR);
    File loggedDir = new File(baseDir, LOGGEDDIR);
    File xmitableDir = new File(baseDir, SENDINGDIR);
    File errDir = new File(baseDir, ERRORDIR);
    Preconditions.checkNotNull(writingDir);
    Preconditions.checkNotNull(loggedDir);
    Preconditions.checkNotNull(xmitableDir);
    Preconditions.checkNotNull(errDir);
    this.importDir = new File(baseDir, IMPORTDIR);
    this.writingDir = writingDir;
    this.loggedDir = loggedDir;
    this.sendingDir = xmitableDir;
    this.errorDir = errDir;
    state = ManagerState.CLOSED;
  }

  synchronized public void open() throws IOException {
    // TODO (jon) be less strict. ?? need to return on and figure out why thisis
    // wrong, add
    // latches.

    // Preconditions.checkState(state == ManagerState.CLOSED,
    // "Must be in CLOSED state to open, currently " + state);

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
    if (!FileUtil.makeDirs(errorDir)) {
      throw new IOException("Unable to create error dir: " + sendingDir);
    }

    state = ManagerState.OPEN;
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

  synchronized public void close() throws IOException {
    if (state == ManagerState.CLOSED) {
      LOG.warn("Double close (which is ok)");
    }

    if (state == ManagerState.CLOSING) {
      LOG.warn("Close while in closing state, odd");
    }

    state = ManagerState.CLOSING;
  }

  /**
   * This looks at directory structure and recovers state based on where files
   * are in the file system.
   * 
   * For a first cut, we will just get everything into the logged state and
   * restart from there. Optimizations will recover at finer grain and be more
   * performant.
   */
  synchronized public void recover() throws IOException {

    // move all writing into the logged dir.
    for (String f : writingDir.list()) {
      File old = new File(writingDir, f);

      if (!old.isFile() || !old.renameTo(new File(loggedDir, f))) {
        throw new IOException("Unable to recover - couldn't rename " + old
            + " to " + loggedDir + f);
      }
      LOG.debug("Recover moved " + f + " from WRITING to LOGGED");
    }

    // move all sending into the logged dir
    for (String f : sendingDir.list()) {
      File old = new File(sendingDir, f);
      if (!old.isFile() || !old.renameTo(new File(loggedDir, f))) {
        throw new IOException("Unable to recover - couldn't rename " + old
            + " to " + loggedDir + f);
      }
      LOG.debug("Recover moved " + f + " from SENDING to LOGGED");
    }

    // add all logged to loggedQ and table
    for (String f : loggedDir.list()) {

      // File log = new File(loggedDir, f);
      DFOData data = DFOData.recovered(f);
      table.put(f, data);
      loggedQ.add(f);
      recoverCount.incrementAndGet();
      LOG.debug("Recover loaded " + f);
    }

    // carry on now on your merry way.
  }

  /**
   * Returns a new sink when the roller asks for a new one.
   */
  synchronized public EventSink newWritingSink(final Tagger tagger)
      throws IOException {
    File dir = getDir(State.WRITING);
    final String tag = tagger.newTag();
    EventSink curSink = new SeqfileEventSink(new File(dir, tag)
        .getAbsoluteFile());
    writingQ.add(tag);
    DFOData data = new DFOData(tag);
    table.put(tag, data);
    writingCount.incrementAndGet();

    return new EventSinkDecorator<EventSink>(curSink) {
      @Override
      synchronized public void append(Event e) throws IOException,
          InterruptedException {
        synchronized (NaiveFileFailoverManager.this) {
          getSink().append(e);
          writingEvtCount.incrementAndGet();
        }
      }

      @Override
      synchronized public void close() throws IOException, InterruptedException {
        synchronized (NaiveFileFailoverManager.this) {
          super.close();
          if (!writingQ.contains(tag)) {
            LOG.warn("Already changed tag " + tag + " out of WRITING state");
            return;
          }
          LOG.info("File lives in " + getFile(tag));

          changeState(tag, State.WRITING, State.LOGGED);
          loggedCount.incrementAndGet();
        }
      }
    };
  }

  /**
   * This instantiates a roller where all input is sent to.
   */
  @Override
  public RollSink getEventSink(final RollTrigger t) throws IOException {
    // NaiveFileFailover is just a place holder
    return new RollSink(new Context(), "NaiveFileFailover", t, 250) {

      @Override
      public EventSink newSink(Context ctx) throws IOException {
        // TODO (jon) clean this up -- want to deprecate Tagger
        return newWritingSink(t.getTagger());
      }

    };
  }

  /**
   * This is private and not thread safe.
   */
  private LinkedBlockingQueue<String> getQueue(State state) {
    Preconditions.checkNotNull(state,
        "Attempted to get queue for invalid null state");
    switch (state) {
    case WRITING:
      return writingQ;
    case LOGGED:
      return loggedQ;
    case SENDING:
      return sendingQ;
    case IMPORT:
    default:
      return null;
    }
  }

  /**
   * This is private and not thread safe.
   */
  private File getDir(State state) {
    Preconditions.checkNotNull(state,
        "Attempted to get dir for invalid null state");
    switch (state) {
    case IMPORT:
      return importDir;
    case WRITING:
      return writingDir;
    case LOGGED:
      return loggedDir;
    case SENDING:
      return sendingDir;
    case ERROR:
      return errorDir;
    default:
      return null;
    }
  }

  /**
   * This is private and not thread safe
   */
  private File getFile(String tag) {
    Preconditions.checkNotNull(tag, "Attempted to get file for empty tag");
    DFOData data = table.get(tag);
    Preconditions.checkNotNull(data, "Data for tag " + tag + " was empty");

    File dir = getDir(data.s);
    return new File(dir, tag);
  }

  /**
   * Cleanup a file after it has been successfully processed.
   * 
   * This can through both IOExceptions and runtime exceptions due to
   * Preconditions failures.
   * 
   * According to the link below, Solaris (I assume POSIX/linux) does atomic
   * rename but Windows does not guarantee it.
   * http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4017593 To be truly
   * correct, I need to check the return value (will likely fail in unix if
   * moving from one volume to another instead of just within same volume)
   */
  synchronized void changeState(String tag, State oldState, State newState)
      throws IOException {
    DFOData data = table.get(tag);
    Preconditions.checkArgument(data != null, "Tag " + tag + " has no data");
    Preconditions.checkArgument(tag.equals(data.tag),
        "Data associated with tag didn't match tag " + tag);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Change " + data.s + "/" + oldState + " to " + newState + " : "
          + tag);
    }

    // null allows any previous state.
    if (oldState == null) {
      oldState = data.s;
    }

    Preconditions.checkState(data.s == oldState, "Expected state to be "
        + oldState + " but was " + data.s);

    if (oldState == State.ERROR) {
      throw new IllegalStateException("Cannot move from error state");
    }

    // SENT is terminal state, no where to move, just delete it.
    if (newState == State.SENT) {
      getQueue(oldState).remove(tag);
      File sentFile = getFile(tag);
      data.s = newState;
      if (!sentFile.delete()) {
        LOG.error("Couldn't delete " + sentFile
            + " - can be safely manually deleted");
      }

      // TODO (jon) need to eventually age off sent files entry to not exhaust
      // memory

      return;
    }
    // move files to other directories to making state change durable.
    File orig = getFile(tag);
    File newf = new File(getDir(newState), tag);
    boolean success = orig.renameTo(newf);
    if (!success) {
      throw new IOException("Move  " + orig + " -> " + newf + "failed!");
    }

    // is successful, update queues.
    LOG.debug("old state is " + oldState);
    getQueue(oldState).remove(tag);
    BlockingQueue<String> q = getQueue(newState);
    if (q != null) {
      q.add(tag);
    }
    data.s = newState;
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

        // TODO(jon) Eat the exception?
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
        LOG.warn("close had a problem " + src, ioe);
        changeState(tag, null, State.ERROR);
        throw ioe; // rethrow this
      }
    }

    @Override
    public Event next() throws IOException, InterruptedException {
      try {
        Event e = src.next();
        if (e != null) {
          readEvtCount.incrementAndGet();
          // TODO make the roll tag a parameter so that we don't have to remove
          // it here.
          e = EventImpl.unselect(e, RollSink.DEFAULT_ROLL_TAG);
        }
        updateEventProcessingStats(e);
        return e;
      } catch (IOException ioe) {
        LOG.warn("next had a problem " + src, ioe);
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
  }

  /**
   * This gets a valid seqfile event source. If open fails, it just cleans that
   * file up and moves on to the next
   * 
   * Will block unless this manager has been told to close. When closed will
   * return null;
   */
  public EventSource getUnsentSource() throws IOException {
    synchronized (this) {
      if (state == ManagerState.CLOSED) {
        return null;
      }
    }

    // need to get a current file?
    String sendingTag = null;
    try {
      while (sendingTag == null) {
        sendingTag = loggedQ.poll(200, TimeUnit.MILLISECONDS);
        // exit condition is when closed is flagged and the queue is empty.

        // this checks on the queues needs to be atomic.
        if (sendingTag == null) {
          synchronized (this) {
            if (state == ManagerState.CLOSING && writingQ.isEmpty()
                && loggedQ.isEmpty() && sendingQ.isEmpty()) {
              state = ManagerState.CLOSED;
              // this manager is now closed and the queues are empty.
              return null;
            }
          }
        }
      }

    } catch (InterruptedException e) {
      LOG.error("interrupted", e);
      throw new IOException(e);
    }

    LOG.info("opening new file for " + sendingTag);
    changeState(sendingTag, State.LOGGED, State.SENDING);
    sendingCount.incrementAndGet();
    File curFile = getFile(sendingTag);
    EventSource curSource = new SeqfileEventSource(curFile.getAbsolutePath());
    return new StateChangeDeco(curSource, sendingTag);
  }

  /**
   * change something that is sent and not acknowledged to logged state so that
   * the normal mechanisms eventually retry sending it.
   */
  synchronized public void retry(String tag) throws IOException {
    // Yuck. This is like a CAS right now.
    DFOData data = table.get(tag);
    if (data != null) {
      if (data.s == State.SENDING || data.s == State.LOGGED) {
        LOG.warn("There was a race that happend with SENT vs SENDING states");
        return;
      }
    }
    changeState(tag, State.SENT, State.LOGGED);
    retryCount.incrementAndGet();
  }

  @Override
  public EventSource getEventSource() throws IOException {
    return new DiskFailoverSource(this);
  }

  /**
   * This is a hook that imports external files to the dfo bypassing the default
   * append
   */
  synchronized public void importData() throws IOException {
    // move all writing into the logged dir.
    for (String fn : importDir.list()) {
      // add to logging queue
      DFOData data = DFOData.recovered(fn);
      synchronized (this) {
        table.put(fn, data);
        loggedQ.add(fn);
        importedCount.incrementAndGet();
      }

    }
  }

  /**
   * Returns true if the dfo log is empty, false if there remain events saved
   * off
   */
  @Override
  public synchronized boolean isEmpty() {
    return writingQ.isEmpty() && loggedQ.isEmpty() && sendingQ.isEmpty();
  }

  @Override
  public String getName() {
    return "NaiveDiskFailover";
  }

  @Override
  synchronized public ReportEvent getReport() {
    ReportEvent rpt = new ReportEvent(getName());

    // historical counts
    rpt.setLongMetric(A_IMPORTED, importedCount.get());
    rpt.setLongMetric(A_WRITING, writingCount.get());
    rpt.setLongMetric(A_LOGGED, loggedCount.get());
    rpt.setLongMetric(A_SENDING, sendingCount.get());
    rpt.setLongMetric(A_ERROR, errCount.get());
    rpt.setLongMetric(A_RECOVERED, recoverCount.get());

    // Waiting to send
    rpt.setLongMetric(A_IN_LOGGED, loggedQ.size());

    // message counts
    rpt.setLongMetric(A_MSG_WRITING, writingEvtCount.get());
    rpt.setLongMetric(A_MSG_READ, readEvtCount.get());
    return rpt;
  }

}
