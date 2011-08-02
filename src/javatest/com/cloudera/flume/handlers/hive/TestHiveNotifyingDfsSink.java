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
package com.cloudera.flume.handlers.hive;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.Event.Priority;
import com.cloudera.flume.handlers.avro.AvroJsonOutputFormat;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;

/**
 * Test the hive notification dfs sink to make sure it sends notifications with
 * valid data.
 */
public class TestHiveNotifyingDfsSink {

  /**
   * This just prints data out about the notifications that have been triggered.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testDefault() throws IOException, InterruptedException {
    File f = FileUtil.mktempdir();

    EventSink snk = new HiveNotifyingDfsSink("file://" + f + "/%Y-%m-%d/",
        "file-%{host}", "hivetable");

    snk.open();
    long day_millis = 1000 * 60 * 60 * 24;
    Event e1 = new EventImpl(new byte[0], Clock.unixTime(), Priority.INFO, 0,
        "localhost");
    Event e2 = new EventImpl(new byte[0], e1.getTimestamp() + day_millis,
        Priority.INFO, 0, "localhost");
    Event e3 = new EventImpl(new byte[0], e1.getTimestamp() + 2 * day_millis,
        Priority.INFO, 0, "localhost");
    snk.append(e1);
    snk.append(e2);
    snk.append(e3);
    snk.close();

    FileUtil.rmr(f);
  }

  /**
   * This installs a custom handler that records the notfications that have been
   * trigered, and checks values to make sure the notification is sane.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testHandler() throws IOException, InterruptedException {
    File f = FileUtil.mktempdir();

    // HiveFileReadyHandler hfrh = mock(HiveFileReadyHandler.class);
    final List<HiveDirCreatedNotification> saves = new ArrayList<HiveDirCreatedNotification>();
    HiveDirCreatedHandler hfrh = new HiveDirCreatedHandler() {
      @Override
      public void handleNotification(HiveDirCreatedNotification notif) {
        saves.add(notif);
      }
    };

    String path = "file://" + f + "/%Y-%m-%d/";
    EventSink snk = new HiveNotifyingDfsSink(path, "file-%{host}", "hivetable",
        new AvroJsonOutputFormat(), hfrh);

    snk.open();
    long day_millis = 1000 * 60 * 60 * 24;
    Event e1 = new EventImpl(new byte[0], Clock.unixTime(), Priority.INFO, 0,
        "localhost");
    Event e2 = new EventImpl(new byte[0], e1.getTimestamp() + day_millis,
        Priority.INFO, 0, "localhost");
    Event e3 = new EventImpl(new byte[0], e1.getTimestamp() + 2 * day_millis,
        Priority.INFO, 0, "localhost");
    snk.append(e1);
    snk.append(e2);
    snk.append(e3);
    snk.close();

    FileUtil.rmr(f);

    assertEquals(3, saves.get(0).meta.size()); // 3 date files, but not host.
    assertEquals("hivetable", saves.get(0).table);

    // there is a notification for each of the three dirs created.
    Set<String> paths = new HashSet<String>();
    paths.add(e1.escapeString(path));
    paths.add(e2.escapeString(path));
    paths.add(e3.escapeString(path));
    assertTrue(paths.remove(saves.get(0).dir));
    assertTrue(paths.remove(saves.get(1).dir));
    assertTrue(paths.remove(saves.get(2).dir));
  }

  /**
   * This tests the deduplication handler to verify that notifications are only
   * sent when new directories are created -- new files in the same dir are not
   * renotified.
   * 
   * @throws InterruptedException
   */
  @Test
  public void testDedupHandler() throws IOException, InterruptedException {
    File f = FileUtil.mktempdir();

    final List<HiveDirCreatedNotification> saves = new ArrayList<HiveDirCreatedNotification>();
    HiveDirCreatedHandler hfrh = new HiveNotifyingDfsSink.DedupDefaultHandler(
        new HiveDirCreatedHandler() {
          @Override
          public void handleNotification(HiveDirCreatedNotification notif) {
            saves.add(notif);
          }
        });

    long day_millis = 1000 * 60 * 60 * 24;
    Event e1 = new EventImpl(new byte[0], Clock.unixTime(), Priority.INFO, 0,
        "localhost");
    Event e2 = new EventImpl(new byte[0], e1.getTimestamp() + day_millis,
        Priority.INFO, 0, "localhost");
    Event e3 = new EventImpl(new byte[0], e1.getTimestamp() + 2 * day_millis,
        Priority.INFO, 0, "localhost");

    String path = "file://" + f + "/%Y-%m-%d/";
    EventSink snk = new HiveNotifyingDfsSink(path, "file-%{host}", "hivetable",
        new AvroJsonOutputFormat(), hfrh);

    snk.open();
    snk.append(e1);
    snk.append(e2);
    snk.append(e3);
    snk.close();

    // Simulate a roll by send the messages a second time sink using the same
    // HiveNewDirNotification handlers.

    // TODO (jon) fix this sink's open close has a problem. here just
    // instantiating a new one
    snk = new HiveNotifyingDfsSink(path, "file-%{host}", "hivetable",
        new AvroJsonOutputFormat(), hfrh);
    snk.open();
    snk.append(e1);
    snk.append(e2);
    snk.append(e3);
    snk.close();

    FileUtil.rmr(f);

    assertEquals(3, saves.get(0).meta.size()); // 3 date files, but not host.
    assertEquals("hivetable", saves.get(0).table);
    Set<String> paths = new HashSet<String>();
    paths.add(e1.escapeString(path));
    paths.add(e2.escapeString(path));
    paths.add(e3.escapeString(path));

    assertTrue(paths.remove(saves.get(0).dir));
    assertTrue(paths.remove(saves.get(1).dir));
    assertTrue(paths.remove(saves.get(2).dir));
  }

}
