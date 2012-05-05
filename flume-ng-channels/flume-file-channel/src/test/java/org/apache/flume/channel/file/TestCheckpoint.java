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

import java.io.File;
import java.io.IOException;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestCheckpoint {

  File file;
  @Before
  public void setup() throws IOException {
    file = File.createTempFile("Checkpoint", "");
    Assert.assertTrue(file.isFile());
    Assert.assertTrue(file.canWrite());
  }
  @After
  public void cleanup() {
    file.delete();
  }
  @Test
  public void testSerialization() throws IOException {
    FlumeEventPointer ptrIn = new FlumeEventPointer(10, 20);
    FlumeEventQueue queueIn = new FlumeEventQueue(1);
    queueIn.addHead(ptrIn);
    Checkpoint checkpoint = new Checkpoint(file, 1);
    Assert.assertEquals(0, checkpoint.getTimestamp());
    checkpoint.write(queueIn);
    FlumeEventQueue queueOut = checkpoint.read();
    FlumeEventPointer ptrOut = queueOut.removeHead();
    Assert.assertEquals(ptrIn, ptrOut);
    Assert.assertTrue(checkpoint.getTimestamp() > 0);
  }
}
