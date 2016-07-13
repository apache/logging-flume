/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.flume.serialization;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

public class TestDurablePositionTracker {

  private static final Logger logger = LoggerFactory.getLogger(TestDurablePositionTracker.class);

  @Test
  public void testBasicTracker() throws IOException {
    File metaFile = File.createTempFile(getClass().getName(), ".meta");
    metaFile.delete();
    File dataFile = File.createTempFile(getClass().getName(), ".data");
    Files.write("line 1\nline2\n", dataFile, Charsets.UTF_8);

    final long NEW_POS = 7;

    PositionTracker tracker;
    tracker = new DurablePositionTracker(metaFile, dataFile.toString());
    Assert.assertEquals(0, tracker.getPosition());
    tracker.storePosition(NEW_POS);
    Assert.assertEquals(NEW_POS, tracker.getPosition());
    tracker.close();

    // target only gets updated if the file did not exist
    tracker = new DurablePositionTracker(metaFile, "foobar");
    Assert.assertEquals(NEW_POS, tracker.getPosition());
    Assert.assertEquals(dataFile.getAbsolutePath(), tracker.getTarget());
  }

  // test a valid file
  @Test
  public void testGoodTrackerFile() throws IOException, URISyntaxException {
    String fileName = "/TestResettableFileInputStream_1.avro";
    File trackerFile = new File(getClass().getResource(fileName).toURI());
    Assert.assertTrue(trackerFile.exists());
    PositionTracker tracker;
    tracker = new DurablePositionTracker(trackerFile, "foo");
    // note: 62 is the last value in this manually-created file
    Assert.assertEquals(62, tracker.getPosition());
  }

  // test a truncated file
  @Test
  public void testPartialTrackerFile() throws IOException, URISyntaxException {
    String fileName = "/TestResettableFileInputStream_1.truncated.avro";
    File trackerFile = new File(getClass().getResource(fileName).toURI());
    Assert.assertTrue(trackerFile.exists());
    PositionTracker tracker;
    tracker = new DurablePositionTracker(trackerFile, "foo");
    // note: 25 is the last VALID value in this manually-created file
    Assert.assertEquals(25, tracker.getPosition());
  }

}
