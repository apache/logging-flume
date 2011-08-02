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

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSource;
import com.cloudera.flume.reporter.ReportEvent;

/**
 * This is a wrapper for the NaiveFileWALManager that blocks if there is no
 * entry in the WAL to read, or otherwise reads sets from the WAL when next is
 * called.
 * 
 * This is purposely package visibility -- the only way to get one of these
 * should be through a NaiveFileWALDeco
 */
class WALSource extends EventSource.Base {
  static final Logger LOG = LoggerFactory.getLogger(WALSource.class);

  final WALManager walMan;
  EventSource curSource;

  WALSource(WALManager walMan) {
    this.walMan = walMan;
    curSource = null;
  }

  EventSource getValidSource() throws IOException {
    while (curSource == null) {
      curSource = walMan.getUnackedSource();
      if (curSource == null) {
        // closed and no more sources
        return null;
      }
      try {
        curSource.open();
      } catch (Exception ex) {
        LOG.warn("Exception opening underlaying source", ex);
        curSource = null;
      }
    }
    return curSource;
  }

  /**
   * Gets the current source and returns next value or null if it is done or
   * encountered an error
   */
  Event getValidNext() {
    try {
      Event e = curSource.next();
      return e;
    } catch (Exception ex) {
      LOG.warn("Exception getting next", ex);
      return null;
    }
  }

  @Override
  public Event next() throws IOException, InterruptedException {
    curSource = getValidSource();
    if (curSource == null)
      return null;

    // read next event
    Event e = getValidNext();
    if (e != null) {
      updateEventProcessingStats(e);
      return e; // successful case
    }

    // no more events? try to get next source
    do {
      // was regular end of source, close current, cleanup, and get next
      // Note: walMan provided source that takes care of managing state
      LOG.info("end of file " + curSource);
      try {
        curSource.close();
      } catch (Exception ex) {
        LOG.warn("Exception closing (just continue)", ex);
      }

      curSource = walMan.getUnackedSource();
      if (curSource == null)
        return null; // no more sources;
      try {
        curSource.open();
      } catch (IOException ex) {
        LOG.warn("Exception opening", ex);
        continue;
      }
      e = getValidNext();
    } while (e == null);
    updateEventProcessingStats(e);
    return e; // return valid event
  }

  @Override
  public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
    super.getReports(namePrefix, reports);
    if (curSource != null) {
      curSource.getReports(namePrefix + getName() + ".", reports);
    }
  }

  public void recover() throws IOException {
    walMan.recover();
  }

  @Override
  public void close() throws IOException {
    walMan.stopDrains();
  }

  @Override
  public void open() throws IOException {
    walMan.open(); // create dirs
    walMan.recover();
  }

}
