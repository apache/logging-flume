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
package org.apache.flume.channel.file;

import static org.apache.flume.channel.file.TestUtils.*;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class TestCheckpointRebuilder extends TestFileChannelBase {

  protected static final Logger LOG = LoggerFactory
      .getLogger(TestCheckpointRebuilder.class);

  @Before
  public void setup() throws Exception {
    super.setup();
  }

  @After
  public void teardown() {
    super.teardown();
  }
  @Test
  public void testFastReplay() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CAPACITY,
        String.valueOf(50));
    overrides.put(FileChannelConfiguration.TRANSACTION_CAPACITY,
        String.valueOf(50));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = fillChannel(channel, "checkpointBulder");
    channel.stop();
    File checkpointFile = new File(checkpointDir, "checkpoint");
    File metaDataFile = Serialization.getMetaDataFile(checkpointFile);
    File inflightTakesFile = new File(checkpointDir, "inflighttakes");
    File inflightPutsFile = new File(checkpointDir, "inflightputs");
    File queueSetDir = new File(checkpointDir, "queueset");
    Assert.assertTrue(checkpointFile.delete());
    Assert.assertTrue(metaDataFile.delete());
    Assert.assertTrue(inflightTakesFile.delete());
    Assert.assertTrue(inflightPutsFile.delete());
    EventQueueBackingStore backingStore =
        EventQueueBackingStoreFactory.get(checkpointFile, 50,
            "test");
    FlumeEventQueue queue = new FlumeEventQueue(backingStore, inflightTakesFile,
          inflightPutsFile, queueSetDir);
    CheckpointRebuilder checkpointRebuilder =
        new CheckpointRebuilder(getAllLogs(dataDirs), queue, true);
    Assert.assertTrue(checkpointRebuilder.rebuild());
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }
}
