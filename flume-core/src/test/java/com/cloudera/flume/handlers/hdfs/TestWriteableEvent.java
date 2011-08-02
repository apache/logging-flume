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
package com.cloudera.flume.handlers.hdfs;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.core.Event.Priority;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.util.Clock;
import com.cloudera.util.NetUtils;

/**
 * Test to make sure Writeables serialize and deserialize to the same values.
 */
public class TestWriteableEvent {

  @Test
  public void testWritableReversible() throws IOException {
    String s = "this is a test string";
    WriteableEvent e = new WriteableEvent(new EventImpl(s.getBytes()));
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bas);
    e.write(out);
    out.flush();
    byte[] stuff = bas.toByteArray();

    WriteableEvent e2 = WriteableEvent.createWriteableEvent(stuff);
    Assert.assertEquals(e.getTimestamp(), e2.getTimestamp());
    Assert.assertEquals(e.getPriority(), e2.getPriority());
    Assert.assertTrue(Arrays.equals(e.getBody(), e2.getBody()));

  }

  /**
   * Test the map serialization and deserialization.
   */
  @Test
  public void testWritableReversibleWithField() throws IOException {
    String s = "this is a test string";
    Map<String, byte[]> fields = new HashMap<String, byte[]>();
    byte[] data = { 'd', 'a', 't', 'a' };
    fields.put("test", data);

    WriteableEvent e = new WriteableEvent(
        new EventImpl(s.getBytes(), Clock.unixTime(), Priority.INFO, Clock
            .nanos(), NetUtils.localhost(), fields));
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bas);
    e.write(out);
    out.flush();
    byte[] stuff = bas.toByteArray();

    WriteableEvent e2 = WriteableEvent.createWriteableEvent(stuff);
    Assert.assertEquals(e.getTimestamp(), e2.getTimestamp());
    Assert.assertEquals(e.getPriority(), e2.getPriority());
    Assert.assertTrue(Arrays.equals(e.getBody(), e2.getBody()));

    byte[] data2 = e2.get("test");
    Assert.assertTrue(Arrays.equals(data, data2));

  }

  /**
   * Test the map serialization and deserialization.
   */
  @Test
  public void testWritableReversibleWithFields() throws IOException {
    String s = "this is a test string";
    Map<String, byte[]> fields = new HashMap<String, byte[]>();
    byte[] data = { 'd', 'a', 't', 'a' };
    fields.put("test", data);
    String val = "more data, with longer value";
    fields.put("moredata", val.getBytes());

    WriteableEvent e = new WriteableEvent(
        new EventImpl(s.getBytes(), Clock.unixTime(), Priority.INFO, Clock
            .nanos(), NetUtils.localhost(), fields));
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bas);
    e.write(out);
    out.flush();
    byte[] stuff = bas.toByteArray();

    WriteableEvent e2 = WriteableEvent.createWriteableEvent(stuff);
    Assert.assertEquals(e.getTimestamp(), e2.getTimestamp());
    Assert.assertEquals(e.getPriority(), e2.getPriority());
    Assert.assertTrue(Arrays.equals(e.getBody(), e2.getBody()));

    byte[] data2 = e2.get("test");
    Assert.assertTrue(Arrays.equals(data, data2));
    Assert.assertTrue(val.equals(new String(e2.get("moredata"))));
  }
}
