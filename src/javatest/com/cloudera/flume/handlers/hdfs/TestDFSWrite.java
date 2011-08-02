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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.junit.Test;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.FlumeConfiguration;
import com.cloudera.flume.conf.SinkFactory.SinkBuilder;
import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.core.EventSink;
import com.cloudera.util.OSUtils;

/**
 * This tests writing to a distributed hdfs. It assumes the namenode is
 * localhost.
 * 
 */
public class TestDFSWrite {
  @Test
  public void testDirectWrite() throws IOException {
    FlumeConfiguration conf = FlumeConfiguration.get();

    Path path = new Path("file:///tmp/testfile");
    FileSystem hdfs = path.getFileSystem(conf);
    hdfs.deleteOnExit(path);

    String STRING = "Hello World";

    // writing
    FSDataOutputStream dos = hdfs.create(path);
    dos.writeUTF(STRING);
    dos.close();

    // reading
    FSDataInputStream dis = hdfs.open(path);
    String s = dis.readUTF();
    System.out.println(s);
    assertEquals(STRING, s);

    dis.close();

    hdfs.close();
  }

  @Test
  public void testHDFSSequenceFileWrite() throws IOException {
    FlumeConfiguration conf = FlumeConfiguration.get();

    Path path = new Path("file:///tmp/testfile");
    FileSystem hdfs = path.getFileSystem(conf);
    hdfs.deleteOnExit(path);

    Event e = new EventImpl("EVENT".getBytes());

    Writer w = SequenceFile.createWriter(hdfs, conf, path,
        WriteableEventKey.class, WriteableEvent.class);

    // writing
    w.append(new WriteableEventKey(e), new WriteableEvent(e));
    w.close();

    FileStatus stats = hdfs.getFileStatus(path);
    assertTrue(stats.getLen() > 0);

    // reading
    SequenceFile.Reader r = new SequenceFile.Reader(hdfs, path, conf);
    WriteableEventKey k = new WriteableEventKey();
    WriteableEvent evt = new WriteableEvent();
    r.next(k, evt);

    assertEquals(evt.getTimestamp(), e.getTimestamp());
    assertEquals(evt.getNanos(), e.getNanos());
    assertEquals(evt.getPriority(), e.getPriority());
    assertTrue(Arrays.equals(evt.getBody(), e.getBody()));

    hdfs.close();
  }

  @Test
  public void testHDFSEventSink() throws IOException {
    FlumeConfiguration conf = FlumeConfiguration.get();
    String str = "file:///tmp/testfile";
    Path path = new Path(str);
    DFSEventSink sink = new DFSEventSink(str);

    FileSystem hdfs = path.getFileSystem(conf);

    sink.open();
    Event e = new EventImpl("EVENT".getBytes());
    sink.append(e);
    sink.close();

    FileStatus stats = hdfs.getFileStatus(path);
    assertTrue(stats.getLen() > 0);

    // // reading
    SequenceFile.Reader r = new SequenceFile.Reader(hdfs, path, conf);
    WriteableEventKey k = new WriteableEventKey();
    WriteableEvent evt = new WriteableEvent();
    r.next(k, evt);

    assertEquals(evt.getTimestamp(), e.getTimestamp());
    assertEquals(evt.getNanos(), e.getNanos());
    assertEquals(evt.getPriority(), e.getPriority());
    assertTrue(Arrays.equals(evt.getBody(), e.getBody()));

    hdfs.close();
  }

  /**
   * Test that pathnames are being correctly substituted.
   */
  @Test
  public void testTaggedWrites() throws IOException {
    FlumeConfiguration conf = FlumeConfiguration.get();
    String template = "file:///tmp/testfile-%{mytag}";
    String real1 = "file:///tmp/testfile-one";
    String real2 = "file:///tmp/testfile-two";

    DFSEventSink sink = new DFSEventSink(template);

    Event e1 = new EventImpl("EVENT1".getBytes());
    e1.set("mytag", "one".getBytes());
    Event e2 = new EventImpl("EVENT2".getBytes());
    e2.set("mytag", "two".getBytes());
    sink.open();
    sink.append(e1);
    sink.append(e2);
    sink.close();

    Path path1 = new Path(real1);
    Path path2 = new Path(real2);

    FileSystem hdfs = path1.getFileSystem(conf);
    FileStatus stats = hdfs.getFileStatus(path1);
    assertTrue("File " + real1 + " not found but expected", stats.getLen() > 0);
    stats = hdfs.getFileStatus(new Path(real2));
    assertTrue("File " + real2 + " not found but expected", stats.getLen() > 0);

    SequenceFile.Reader r = new SequenceFile.Reader(hdfs, path1, conf);
    WriteableEventKey k = new WriteableEventKey();
    WriteableEvent evt = new WriteableEvent();
    r.next(k, evt);

    assertEquals(evt.getTimestamp(), e1.getTimestamp());
    assertEquals(evt.getNanos(), e1.getNanos());
    assertEquals(evt.getPriority(), e1.getPriority());
    assertTrue(Arrays.equals(evt.getBody(), e1.getBody()));

    r = new SequenceFile.Reader(hdfs, path2, conf);
    k = new WriteableEventKey();
    evt = new WriteableEvent();
    r.next(k, evt);

    assertEquals(evt.getTimestamp(), e2.getTimestamp());
    assertEquals(evt.getNanos(), e2.getNanos());
    assertEquals(evt.getPriority(), e2.getPriority());
    assertTrue(Arrays.equals(evt.getBody(), e2.getBody()));

    hdfs.close();
  }

  // // Why does this test fail? It doesn't make sense.

  // // an old version used code similar to this and failed. Want to
  // // preserve this to see if it is a bug.
  // // TODO (jon) Why does this fail to write data?
  @Test
  public void testWhyFail() throws IOException {

    // There a was a failure case using :
    FlumeConfiguration conf = FlumeConfiguration.get();
    Path path = new Path("file:///tmp/testfile");
    FileSystem hdfs = path.getFileSystem(conf);

    // writing
    FSDataOutputStream dos = hdfs.create(path);
    hdfs.deleteOnExit(path);

    // this version's Writer has ownOutputStream=false.
    Writer writer = SequenceFile.createWriter(conf, dos,
        WriteableEventKey.class, WriteableEvent.class,
        SequenceFile.CompressionType.NONE, new DefaultCodec());

    Event e = new EventImpl("EVENT".getBytes());

    writer.append(new WriteableEventKey(e), new WriteableEvent(e));
    writer.sync();
    writer.close();

    dos.close(); // It is strange that I have to close the underlying
    // FSDataOutputStream.

    // WTF: nothing written by this writer!
    FileStatus stats = hdfs.getFileStatus(path);
    assertTrue(stats.getLen() > 0);
    // it should have written something but it failed.
  }

  /**
   * No exception expected
   */
  @Test
  public void testBadArgsParseOK() {
    SinkBuilder sb = DFSEventSink.builder();
    sb.build(new Context(), "/foo/msgs");
  }

  /**
   * wrong # args, exception expected
   */
  @Test(expected = IllegalArgumentException.class)
  public void testBadArgsParseOkInstanceFail() {
    SinkBuilder sb = DFSEventSink.builder();
    sb.build(new Context());
  }

  /**
   * Failure occurs when opened due to permissions.
   */
  @Test
  public void testBadArgsOpenFail() throws IOException {
    assumeTrue(!OSUtils.isWindowsOS());

    SinkBuilder sb = DFSEventSink.builder();
    EventSink snk = sb.build(new Context(), "/foo/msgs");
    try {
      snk.open();
    } catch (IOException e) {
      return;
    }
    fail("expected exception due to perms");

  }
}
