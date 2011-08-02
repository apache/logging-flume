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
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.core.Attributes;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.handlers.endtoend.AckChecksumInjector;
import com.cloudera.flume.handlers.endtoend.AckListener;
import com.cloudera.flume.handlers.hdfs.SeqfileEventSink;
import com.cloudera.flume.handlers.hdfs.SeqfileEventSource;
import com.cloudera.flume.handlers.rolling.RollSink;
import com.cloudera.flume.handlers.rolling.RollTrigger;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.flume.reporter.ReportEvent;
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
 */
public class NaiveFileWALManager implements WALManager {
  static Logger LOG = Logger.getLogger(NaiveFileWALManager.class.getName());

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

  private volatile boolean closed = false;

  /**
   * Simple record for keeping the state of tag.
   */
  static class WALData {
    State s;
    String tag;

    WALData(String tag) {
      this.s = State.WRITING;
      this.tag = tag;
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

  synchronized public void open() throws IOException {
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

    closed = false;
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
  synchronized public void stopDrains() throws IOException {
    closed = true;
  }

  /**
   * This looks at directory structure and recovers state based on where files
   * are in the file system.
   * 
   * For a first cut, we will just get everything into the logged state and
   * restart from there. Optimizations can recover at finer grain and be more
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

    // move all sent into the logged dir.
    for (String f : sentDir.list()) {
      File old = new File(sentDir, f);
      if (!old.isFile() || !old.renameTo(new File(loggedDir, f))) {
        throw new IOException("Unable to recover - couldn't rename " + old
            + " to " + loggedDir + f);
      }
      LOG.debug("Recover moved " + f + " from SENT to LOGGED");

    }

    // add all logged to loggedQ and table
    for (String f : loggedDir.list()) {
      // File log = new File(loggedDir, f);
      WALData data = WALData.recovered(f);
      table.put(f, data);
      loggedQ.add(f);
      recoverCount.incrementAndGet();
      LOG.debug("Recover loaded " + f);
    }

    // carry on now on your merry way.
  }

  /**
   * This gets a new sink when rolling, and is called when rolling to a new
   * file.
   */
  synchronized public EventSink newAckWritingSink(final Tagger tagger,
      AckListener al) throws IOException {
    File dir = getDir(State.WRITING);
    final String tag = tagger.newTag();

    EventSink bareSink = new SeqfileEventSink(new File(dir, tag)
        .getAbsoluteFile());
    EventSink curSink = new AckChecksumInjector<EventSink>(bareSink, tag
        .getBytes(), al);

    writingQ.add(tag);
    WALData data = new WALData(tag);
    table.put(tag, data);
    writingCount.incrementAndGet();

    return new EventSinkDecorator<EventSink>(curSink) {
      @Override
      public void append(Event e) throws IOException {
        getSink().append(e);
      }

      @Override
      public void close() throws IOException {
        super.close();
        synchronized (NaiveFileWALManager.this) {
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
   * Returns a new sink when the roller asks for a new one.
   */
  synchronized public EventSink newWritingSink(final Tagger tagger)
      throws IOException {
    File dir = getDir(State.WRITING);
    final String tag = tagger.newTag();
    EventSink curSink = new SeqfileEventSink(new File(dir, tag)
        .getAbsoluteFile());
    writingQ.add(tag);
    WALData data = new WALData(tag);
    table.put(tag, data);

    return new EventSinkDecorator<EventSink>(curSink) {
      @Override
      public void append(Event e) throws IOException {
        if (LOG.isDebugEnabled()) {
          LOG.debug(e); // performance sensitive
        }
        getSink().append(e);

      }

      @Override
      public void close() throws IOException {
        super.close();
        changeState(tag, State.WRITING, State.LOGGED);

      }
    };
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
  synchronized void changeState(String tag, State oldState, State newState)
      throws IOException {
    WALData data = table.get(tag);
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

    /*
     * This uses java's File.rename and File.delete method. According to the
     * link below, Solaris (I assume POSIX/linux) does atomic rename but Windows
     * does not guarantee it.
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
      LOG.debug("Deleting WAL file: " + newf.getAbsoluteFile());
      boolean res = newf.delete();
      if (!res) {
        LOG.warn("Failed to delete complete WAL file: "
            + newf.getAbsoluteFile());

      }
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
    public void open() throws IOException {
      try {
        src.open();
      } catch (IOException ioe) {
        changeState(tag, State.SENDING, State.ERROR);
        errCount.incrementAndGet();
        throw ioe;
      }
    }

    @Override
    public void close() throws IOException {
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
    public Event next() throws IOException {
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
  public EventSource getUnackedSource() throws IOException {
    // need to get a current file?
    String sendingTag = null;
    try {
      while (sendingTag == null) {
        sendingTag = loggedQ.poll(200, TimeUnit.MILLISECONDS);
        // exit condition is when closed is flagged and the queue is empty.

        if (sendingTag == null) {
          synchronized (this) {
            if (closed && loggedQ.isEmpty() && sendingQ.isEmpty())
              return null;
          }
        }
      }
    } catch (InterruptedException e) {
      LOG.error("interrupted", e);
      throw new IOException(e);
    }

    LOG.info("opening log file  " + sendingTag);
    changeState(sendingTag, State.LOGGED, State.SENDING);
    sendingCount.incrementAndGet();
    File curFile = getFile(sendingTag);
    EventSource curSource = new SeqfileEventSource(curFile.getAbsolutePath());
    return new StateChangeDeco(curSource, sendingTag);
  }

  /**
   * Convert a tag from Sent to end-to-end acked.
   */
  synchronized public void toAcked(String tag) throws IOException {
    changeState(tag, State.SENT, State.E2EACKED);
    ackedCount.incrementAndGet();
  }

  /**
   * Change something that is sent and not acknowledged to logged state so that
   * the normal mechanisms will eventually retry sending it.
   */
  synchronized public void retry(String tag) throws IOException {
    // Yuck. This is like a CAS right now.
    WALData data = table.get(tag);
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
      synchronized (this) {
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
  synchronized public ReportEvent getReport() {
    ReportEvent rpt = new ReportEvent(getName());

    // historical counts
    Attributes.setLong(rpt, A_IMPORTED, importedCount.get());
    Attributes.setLong(rpt, A_WRITING, writingCount.get());
    Attributes.setLong(rpt, A_LOGGED, loggedCount.get());
    Attributes.setLong(rpt, A_SENDING, sendingCount.get());
    Attributes.setLong(rpt, A_SENT, sentCount.get());
    Attributes.setLong(rpt, A_ACKED, ackedCount.get());
    Attributes.setLong(rpt, A_RETRY, retryCount.get());
    Attributes.setLong(rpt, A_ERROR, errCount.get());
    Attributes.setLong(rpt, A_RECOVERED, recoverCount.get());

    // Waiting to send
    Attributes.setLong(rpt, A_IN_LOGGED, loggedQ.size());

    // waiting for ack
    Attributes.setLong(rpt, A_IN_SENT, sentQ.size());

    return rpt;
  }

  @Override
  synchronized public boolean isEmpty() {
    return writingQ.isEmpty() && loggedQ.isEmpty() && sendingQ.isEmpty()
        && sentQ.isEmpty();
  }

}
