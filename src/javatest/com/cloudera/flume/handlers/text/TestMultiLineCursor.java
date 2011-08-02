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
package com.cloudera.flume.handlers.text;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.handlers.text.CustomDelimCursor.ByteBufferAsCharSequence;
import com.cloudera.flume.handlers.text.CustomDelimCursor.DelimMode;
import com.cloudera.util.Clock;
import com.cloudera.util.OSUtils;

public class TestMultiLineCursor {
  public static final org.slf4j.Logger LOG = LoggerFactory
      .getLogger(TestMultiLineCursor.class);

  @Test
  public void testByteBufferCharSequence() {
    ByteBuffer buf = ByteBuffer.wrap("This is a test".getBytes());
    ByteBufferAsCharSequence bbcs = new ByteBufferAsCharSequence(buf);

    assertEquals('T', bbcs.charAt(0));
    assertEquals('h', bbcs.charAt(1));
    assertEquals('i', bbcs.charAt(2));
    assertEquals('s', bbcs.charAt(3));
    assertEquals('s', bbcs.charAt(12));
    assertEquals('t', bbcs.charAt(13));

  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testByteBufferCharSequenceIOOBE() {
    ByteBuffer buf = ByteBuffer.wrap("This is a test".getBytes());
    ByteBufferAsCharSequence bbcs = new ByteBufferAsCharSequence(buf);
    bbcs.charAt(14);
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testByteBufferCharSequenceIOOBE2() {
    ByteBuffer buf = ByteBuffer.wrap("This is a test".getBytes());
    ByteBufferAsCharSequence bbcs = new ByteBufferAsCharSequence(buf);
    bbcs.charAt(-1);
  }

  @Test
  public void testBBCSSubsequence() {
    ByteBuffer buf = ByteBuffer.wrap("This is a test".getBytes());
    ByteBufferAsCharSequence bbcs = new ByteBufferAsCharSequence(buf);

    CharSequence cs = bbcs.subSequence(5, 13);
    assertEquals('i', cs.charAt(0));
    assertEquals('s', cs.charAt(1));
    assertEquals('s', cs.charAt(7));

    // original still sane?
    assertEquals('T', bbcs.charAt(0));
    assertEquals('t', bbcs.charAt(13));

  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void testBBCSSubsequenceIOOBE() {
    ByteBuffer buf = ByteBuffer.wrap("This is a test".getBytes());
    ByteBufferAsCharSequence bbcs = new ByteBufferAsCharSequence(buf);

    CharSequence cs = bbcs.subSequence(5, 13);
    cs.charAt(8);
  }

  @Before
  public void setDebug() {
    Logger.getLogger(TailSource.class).setLevel(Level.DEBUG);
  }

  File createDataFile(int count) throws IOException {
    File f = File.createTempFile("tail", ".tmp");
    f.deleteOnExit();
    FileWriter fw = new FileWriter(f);
    for (int i = 0; i < count; i++) {
      fw.write("test " + i + "blah");
      fw.flush();
    }
    fw.close();
    return f;
  }

  void appendData(File f, int start, int count) throws IOException {
    FileWriter fw = new FileWriter(f, true);
    for (int i = start; i < start + count; i++) {
      fw.write("test " + i + "blah");
      fw.flush();
    }
    fw.close();
  }

  /**
   * Pre-existing file, start cursor, and check we get # of events we expected
   */
  @Test
  public void testCursorPreexisting() throws IOException, InterruptedException {
    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f = createDataFile(5);
    Cursor c = new CustomDelimCursor(q, f, "blah", DelimMode.EXCLUDE);

    assertTrue(c.tailBody()); // open the file
    assertTrue(c.tailBody()); // read the file
    assertFalse(c.tailBody()); // no new data.
    assertEquals(5, q.size()); // should be 5 in queue.
  }

  /**
   * Have a pre-existing file, rename th efile, make sure still follow handle.
   **/
  @Test
  public void testCursorMovedPreexisting() throws IOException,
      InterruptedException {
    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    File f2 = File.createTempFile("move", ".tmp");
    f2.delete();
    f2.deleteOnExit();
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f = createDataFile(5);
    Cursor c = new CustomDelimCursor(q, f, "blah", DelimMode.EXCLUDE);

    assertTrue(c.tailBody()); // open the file

    f.renameTo(f2); // move the file (should be no problem).

    assertTrue(c.tailBody()); // finish reading the file
    assertEquals(5, q.size()); // should be 5 in queue.

    assertFalse(c.tailBody()); // No more to read
    assertEquals(5, q.size()); // should be 5 in queue.

    assertFalse(c.tailBody()); // attempt to open file again.

  }

  /**
   * This test case exercises the weakness of the current tail implementation,
   * due to not having an java API to get an inode for a particular file. This
   * new implementation is about as close as we can get.
   * 
   * It is incorrect behavior but should be rare because it requires a file to
   * change at a higher frequency than generally expected and because file would
   * have to have exactly the same length, and because there is only a small
   * window where this is possible.
   */
  @Test
  @Ignore("When a file rotates in with the same size, we cannot tell!")
  public void testCursorRotatePreexistingFailure() throws IOException,
      InterruptedException {
    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    File f2 = File.createTempFile("move", ".tmp");
    f2.delete();
    f2.deleteOnExit();
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f = createDataFile(5);
    Cursor c = new CustomDelimCursor(q, f, "blah", DelimMode.EXCLUDE);

    assertTrue(c.tailBody()); // open the file

    f.renameTo(f2); // move the file (should be no problem).

    // wait a second to force a new last modified time.
    Clock.sleep(1000);
    appendData(f, 5, 5);

    assertTrue(c.tailBody()); // finish reading the first file
    assertEquals(5, q.size()); // should be 5 in queue.

    // If we had Inode info we could detect if it is a different file, however,
    // we don't. Thus, this tail cannot tell that a new file with the same
    // size is different with available metadata.
    assertTrue(c.tailBody()); // attempt to open file again.
    assertTrue(c.tailBody()); // read 2nd file.
    // This should be 10, but actually results in 5.
    assertEquals(5, q.size()); // should be 5 in queue.
  }

  /**
   * rotate with a new file that is longer than the original.
   **/
  @Test
  public void testCursorRotatePreexistingNewLonger() throws IOException,
      InterruptedException {

    // Windows rename semantics different than unix
    Assume.assumeTrue(!OSUtils.isWindowsOS());

    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    File f2 = File.createTempFile("move", ".tmp");
    f2.delete();
    f2.deleteOnExit();
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f = createDataFile(5);
    Cursor c = new CustomDelimCursor(q, f, "blah", DelimMode.EXCLUDE);

    assertTrue(c.tailBody()); // open the file

    f.renameTo(f2); // move the file (should be no problem).

    // wait a second to force a new last modified time.
    Clock.sleep(1000);
    appendData(f, 5, 6); // should be a new file.

    assertTrue(c.tailBody()); // finish reading the first file
    assertEquals(5, q.size()); // should be 5 in queue.

    assertTrue(c.tailBody()); // notice raflen!= filelen, reset
    assertTrue(c.tailBody()); // open new file
    assertTrue(c.tailBody()); // read
    assertFalse(c.tailBody()); // no more to read
    assertEquals(11, q.size()); // should be 5 in queue.

    assertFalse(c.tailBody()); // no change this time
    assertEquals(11, q.size());
  }

  /**
   * rotate with a new file that is shorter. This works
   */
  @Test
  public void testCursorRotatePreexistingNewShorter() throws IOException,
      InterruptedException {

    // Windows rename semantics different than unix
    Assume.assumeTrue(!OSUtils.isWindowsOS());

    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    File f2 = File.createTempFile("move", ".tmp");
    f2.delete();
    f2.deleteOnExit();
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f = createDataFile(5);
    Cursor c = new CustomDelimCursor(q, f, "blah", DelimMode.EXCLUDE);

    assertTrue(c.tailBody()); // open the file

    f.renameTo(f2); // move the file (should be no problem).

    // wait a second to force a new last modified time.
    Clock.sleep(1000);
    appendData(f, 5, 4);

    assertTrue(c.tailBody()); // finish reading the first file
    assertEquals(5, q.size()); // should be 5 in queue.

    assertTrue(c.tailBody()); // notice file rotation, reset
    assertTrue(c.tailBody()); // attempt to open file again.
    assertTrue(c.tailBody()); // read 4 lines from new file
    assertFalse(c.tailBody()); // no more to read

    assertEquals(9, q.size()); // should be 5 + 4 in queue.
  }

  /**
   * This has an explicit deletion between creating a new file with the same
   * size.
   */
  @Test
  public void testCursorRotatePreexistingSameSizeWithDelete()
      throws IOException, InterruptedException {

    // Windows rename semantics different than unix
    Assume.assumeTrue(!OSUtils.isWindowsOS());

    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    File f2 = File.createTempFile("move", ".tmp");
    f2.delete();
    f2.deleteOnExit();
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f = createDataFile(5);
    Cursor c = new CustomDelimCursor(q, f, "blah", DelimMode.EXCLUDE);

    assertTrue(c.tailBody()); // open the file

    f.renameTo(f2); // move the file (should be no problem).

    assertTrue(c.tailBody()); // finish reading the first file
    assertEquals(5, q.size()); // should be 5 in queue.

    assertFalse(c.tailBody()); // attempt to file again. (not there, no
    // progress)

    // wait a second to force a new last modified time.
    Clock.sleep(1000);
    appendData(f, 5, 5);

    assertTrue(c.tailBody()); // open the new file

    assertTrue(c.tailBody()); // read new file
    assertFalse(c.tailBody()); // fails this time
    assertEquals(10, q.size());
  }

  /**
   * Here, we complete reading a file and then replace it with a new file that
   * has a different modification time.
   */
  @Test
  public void testCursorRotatePreexistingSameSizeWithNewModtime()
      throws IOException, InterruptedException {

    // Windows rename semantics different than unix
    Assume.assumeTrue(!OSUtils.isWindowsOS());

    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    File f2 = File.createTempFile("move", ".tmp");
    f2.delete();
    f2.deleteOnExit();
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f = createDataFile(5);
    Cursor c = new CustomDelimCursor(q, f, "blah", DelimMode.EXCLUDE);

    assertTrue(c.tailBody()); // open the file
    assertTrue(c.tailBody()); // finish reading the first file
    assertEquals(5, q.size()); // should be 5 in queue.

    assertFalse(c.tailBody()); // no more to read
    assertFalse(c.tailBody()); // no more to read

    // wait a second to force a new last modified time.
    f.renameTo(f2); // move the file (should be no problem).
    Clock.sleep(1000);
    appendData(f, 5, 5);

    assertTrue(c.tailBody()); // notice new mod time and reset, file has data to
                              // read
    assertTrue(c.tailBody()); // open the new file
    assertTrue(c.tailBody()); // read new file
    assertFalse(c.tailBody()); // no more to read
    assertEquals(10, q.size());
  }

  /**
   * read, write to file, read more
   */
  @Test
  public void testCursorNewAppendPreexisting() throws IOException,
      InterruptedException {
    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(10);
    File f = createDataFile(5);
    f.deleteOnExit();
    Cursor c = new CustomDelimCursor(q, f, "blah", DelimMode.EXCLUDE);

    assertTrue(c.tailBody()); // open the file

    assertTrue(c.tailBody()); // finish reading the file

    appendData(f, 5, 5);

    assertTrue(c.tailBody()); // attempt to open file again.
    assertEquals(10, q.size()); // should be 5 in queue.
  }

  /**
   * no file, file appears, read
   */
  @Test
  public void testCursorFileAppear() throws IOException, InterruptedException {
    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(10);
    File f = File.createTempFile("appear", ".tmp");
    f.delete();
    f.deleteOnExit();
    Cursor c = new CustomDelimCursor(q, f, "blah", DelimMode.EXCLUDE);

    assertFalse(c.tailBody()); // attempt to open, nothing there.
    assertFalse(c.tailBody()); // attempt to open, nothing there.
    assertEquals(0, c.lastChannelSize);
    assertEquals(null, c.in);

    appendData(f, 0, 5);
    assertTrue(c.tailBody()); // finish reading the file
    assertEquals(0, c.lastChannelPos);
    assertTrue(null != c.in);

    assertTrue(c.tailBody()); // finish reading the file
    assertTrue(0 != c.lastChannelSize);
    assertTrue(null != c.in);

    assertFalse(c.tailBody()); // attempt to open file again.
    assertEquals(5, q.size()); // should be 5 in queue.
  }

  /**
   * read end of file with no newline
   */
  @Test
  public void testCursorFileNoNL() throws IOException, InterruptedException {
    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(10);
    File f = File.createTempFile("appear", ".tmp");
    f.delete();
    f.deleteOnExit();
    Cursor c = new CustomDelimCursor(q, f, "blah", DelimMode.EXCLUDE);

    assertFalse(c.tailBody()); // attempt to open, nothing there.
    assertFalse(c.tailBody()); // attempt to open, nothing there.
    assertEquals(0, c.lastChannelSize);
    assertEquals(null, c.in);

    FileWriter fw = new FileWriter(f);
    fw.append("No new line");
    fw.close();

    assertTrue(c.tailBody()); // find and load file
    assertEquals(0, c.lastChannelPos);
    assertTrue(null != c.in);

    assertTrue(c.tailBody()); // read but since of EOL, buffer (no progress)
    assertEquals(0, q.size()); // no events since no EOL found
    assertTrue(0 != c.lastChannelSize);
    assertTrue(null != c.in);

    assertFalse(c.tailBody()); // try to read, but in buffer, no progress

    c.flush();
    assertEquals(1, q.size());

    boolean append = true;
    FileWriter fw2 = new FileWriter(f, append);
    fw2.append("more no new line");
    fw2.close();

    assertTrue(c.tailBody()); // open file
    assertTrue(0 != c.lastChannelSize);
    assertTrue(null != c.in);

    assertTrue(c.tailBody()); // read file.
    assertEquals(1, q.size());
    c.flush();
    assertEquals(2, q.size());

  }

  /**
   * file truncated. no file, file appears, read
   */
  @Test
  public void testCursorTruncate() throws IOException, InterruptedException {
    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(10);
    File f = createDataFile(5);
    Cursor c = new CustomDelimCursor(q, f, "blah", DelimMode.EXCLUDE);

    assertTrue(c.tailBody()); // open file the file
    assertEquals(0, c.lastChannelPos);
    assertTrue(null != c.in);

    assertTrue(c.tailBody()); // finish reading the file
    assertTrue(0 != c.lastChannelSize);
    assertTrue(null != c.in);

    assertFalse(c.tailBody()); // attempt to open file again.
    assertEquals(5, q.size()); // should be 5 in queue.

    // truncate the file -- there will be 1 full event and one unproperly closed
    // event.
    RandomAccessFile raf = new RandomAccessFile(f, "rw");
    raf.setLength(10);
    raf.close();

    assertFalse(c.tailBody()); // detect file truncation, no data to read
    assertEquals(5, q.size()); // should be 5 in queue.
    assertEquals(10, c.lastChannelPos);
    assertTrue(null != c.in);

    assertFalse(c.tailBody()); // no data to read
    assertEquals(5, q.size()); // should be 5 in queue.

    appendData(f, 5, 5); // appending data after truncation

    assertTrue(c.tailBody()); // reading appended data
    assertEquals(10, q.size()); // should be 5 + 5 in queue.

    assertFalse(c.tailBody()); // no data to read
    assertEquals(10, q.size()); // should be 5 + 5 in queue.
  }

  /**
   * multiple files
   */
  @Test
  public void testMultiCursor() throws IOException, InterruptedException {

    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f1 = createDataFile(5);
    Cursor c1 = new CustomDelimCursor(q, f1, "blah", DelimMode.EXCLUDE);

    File f2 = createDataFile(5);
    Cursor c2 = new CustomDelimCursor(q, f2, "blah", DelimMode.EXCLUDE);

    assertTrue(c1.tailBody()); // open the file
    assertTrue(c2.tailBody()); // open the file
    assertTrue(c1.tailBody()); // open the file
    assertTrue(c2.tailBody()); // open the file
    assertFalse(c1.tailBody()); // read the file
    assertFalse(c2.tailBody()); // no new data.
    assertEquals(10, q.size()); // should be 5 in queue.

    appendData(f1, 5, 5);
    assertTrue(c1.tailBody()); // open the file
    assertFalse(c2.tailBody()); // open the file
    assertEquals(15, q.size()); // should be 5 in queue.

    appendData(f2, 5, 5);
    assertFalse(c1.tailBody()); // open the file
    assertTrue(c2.tailBody()); // open the file
    assertEquals(20, q.size()); // should be 5 in queue.

    assertFalse(c1.tailBody()); // open the file
    assertFalse(c2.tailBody()); // open the file

  }

  // ///////////////////////////////////////////////////////////////////

  @Test
  public void testDelimIncludeNext() throws IOException, InterruptedException {
    File f = File.createTempFile("tail", ".tmp");
    f.deleteOnExit();
    FileWriter fw = new FileWriter(f);
    fw.append("2011-03-07 00:26:53,918 [exec-thread] WARN commands.SetChokeLimitForm: PhysicalNode: physNode not present yet!\n"
        + "2011-03-07 00:26:54,920 [Thrift server:class org.apache.thrift.TProcessorFactory on class org.apache.thrift.transport.TSaneServerSocket]"
        + " WARN server.TSaneThreadPoolServer: Transport error occurred during acceptance of message.\n"
        + "org.apache.thrift.transport.TTransportException: java.net.SocketException: Socket closed\n"
        + "      at org.apache.thrift.transport.TSaneServerSocket.acceptImpl(TSaneServerSocket.java:137)\n"
        + "      at org.apache.thrift.transport.TServerTransport.accept(TServerTransport.java:31)\n"
        + "      at org.apache.thrift.server.TSaneThreadPoolServer$1.run(TSaneThreadPoolServer.java:175)\n"
        + "Caused by: java.net.SocketException: Socket closed\n"
        + "      at java.net.PlainSocketImpl.socketAccept(Native Method)\n"
        + "      at java.net.PlainSocketImpl.accept(PlainSocketImpl.java:408)\n"
        + "      at java.net.ServerSocket.implAccept(ServerSocket.java:462)\n"
        + "      at java.net.ServerSocket.accept(ServerSocket.java:430)\n"
        + "      at org.apache.thrift.transport.TSaneServerSocket.acceptImpl(TSaneServerSocket.java:132)\n"
        + "      ... 2 more\n"
        + "2011-03-07 00:26:55,924 [main] INFO server.ThriftReportServer: Stopping ReportServer...");
    fw.close();

    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    Cursor c1 = new CustomDelimCursor(q, f, "\\n\\d\\d\\d\\d",
        DelimMode.INCLUDE_NEXT);
    assertTrue(c1.tailBody());
    assertTrue(c1.tailBody());
    assertFalse(c1.tailBody());
    c1.flush();

    // output for debugging.
    List<byte[]> l = new ArrayList<byte[]>(3);
    while (!q.isEmpty()) {
      System.out.println("====");
      byte bs[] = q.poll().getBody();
      l.add(bs);
      System.out.println(new String(bs));
    }

    // should be three events
    assertEquals(3, l.size());
    assertEquals(110, l.get(0).length);
    assertEquals(1008, l.get(1).length);
    assertEquals(88, l.get(2).length);
  }

  @Test
  public void testDelimIncludePrev() throws IOException, InterruptedException {
    File f = File.createTempFile("tail", ".tmp");
    f.deleteOnExit();
    FileWriter fw = new FileWriter(f);
    fw.append("<?xml version=\"1.0\"?><?xml-stylesheet type=\"text/xsl\"  href=\"logs.xsl\"?>"
        + "<a><b><c/></b><b><c/></b></a>");
    fw.close();

    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    Cursor c1 = new CustomDelimCursor(q, f, "</b>", DelimMode.INCLUDE_PREV);
    assertTrue(c1.tailBody());
    assertTrue(c1.tailBody());
    assertFalse(c1.tailBody());
    c1.flush();

    // output for debugging.
    List<byte[]> l = new ArrayList<byte[]>(3);
    while (!q.isEmpty()) {
      System.out.println("====");
      byte bs[] = q.poll().getBody();
      l.add(bs);
      System.out.println(new String(bs));
    }

    // should be three events
    assertEquals(3, l.size());
    assertEquals(86, l.get(0).length);
    assertEquals(11, l.get(1).length);
    assertEquals(4, l.get(2).length);
  }

  @Test
  public void testDelimExclude() throws IOException, InterruptedException {
    File f = File.createTempFile("tail", ".tmp");
    f.deleteOnExit();
    FileWriter fw = new FileWriter(f);
    fw.append("a\nb\n\n" + "c\nd\ne\n\n\n" + "f\ng\n\n\n\n" + "h");
    fw.close();

    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    // must have more than 1 \n, + is greedy (consumes longest possible match)
    Cursor c1 = new CustomDelimCursor(q, f, "\n\n+", DelimMode.EXCLUDE);
    assertTrue(c1.tailBody());
    assertTrue(c1.tailBody());
    assertFalse(c1.tailBody());
    c1.flush();

    // output for debugging.
    List<byte[]> l = new ArrayList<byte[]>(3);
    while (!q.isEmpty()) {
      System.out.println("====");
      byte bs[] = q.poll().getBody();
      l.add(bs);
      System.out.println(new String(bs));
    }

    // should be three events
    assertEquals(4, l.size());
    assertEquals(3, l.get(0).length);
    assertEquals(5, l.get(1).length);
    assertEquals(3, l.get(2).length);
    assertEquals(1, l.get(3).length);

  }
}
