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
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mortbay.log.Log;

import com.cloudera.flume.core.Event;
import com.cloudera.util.Clock;
import com.cloudera.util.FileUtil;
import com.cloudera.util.OSUtils;

/**
 * Test the new and improved TailSource cursor. It also shows some test code
 * that tests the java api, showing why the inability to get an inode limits a
 * java tail implementation unless it goes to jni or exec calls.
 */
public class TestTailSourceCursor {

  @Before
  public void setDebug() {
    Logger.getLogger(TailSource.class).setLevel(Level.DEBUG);
  }

  File createDataFile(int count) throws IOException {
    File f = FileUtil.createTempFile("tail", ".tmp");
    f.deleteOnExit();
    FileWriter fw = new FileWriter(f);
    for (int i = 0; i < count; i++) {
      fw.write("test " + i + "\n");
      fw.flush();
    }
    fw.close();
    return f;
  }

  void appendData(File f, int start, int count) throws IOException {
    FileWriter fw = new FileWriter(f, true);
    for (int i = start; i < start + count; i++) {
      fw.write("test " + i + "\n");
      fw.flush();
    }
    fw.close();
  }

  /**
   * Pre-existing file, start cursor, and check we get # of events we expected
   */
  @Test
  public void testCursorPrexisting() throws IOException, InterruptedException {
    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f = createDataFile(5);
    Cursor c = new Cursor(q, f);

    assertTrue(c.tailBody()); // open the file
    assertTrue(c.tailBody()); // read the file
    assertFalse(c.tailBody()); // no new data.
    assertEquals(5, q.size()); // should be 5 in queue.
  }

  /**
   * Have a pre-existing file, rename th efile, make sure still follow handle.
   **/
  @Test
  public void testCursorMovedPrexisting() throws IOException,
      InterruptedException {
    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    File f2 = FileUtil.createTempFile("move", ".tmp");
    f2.delete();
    f2.deleteOnExit();
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f = createDataFile(5);
    Cursor c = new Cursor(q, f);

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
  public void testCursorRotatePrexistingFailure() throws IOException,
      InterruptedException {
    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    File f2 = FileUtil.createTempFile("move", ".tmp");
    f2.delete();
    f2.deleteOnExit();
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f = createDataFile(5);
    Cursor c = new Cursor(q, f);

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
  public void testCursorRotatePrexistingNewLonger() throws IOException,
      InterruptedException {

    // Windows rename semantics different than unix
    Assume.assumeTrue(!OSUtils.isWindowsOS());

    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    File f2 = FileUtil.createTempFile("move", ".tmp");
    f2.delete();
    f2.deleteOnExit();
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f = createDataFile(5);
    Cursor c = new Cursor(q, f);

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
  public void testCursorRotatePrexistingNewShorter() throws IOException,
      InterruptedException {

    // Windows rename semantics different than unix
    Assume.assumeTrue(!OSUtils.isWindowsOS());

    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    File f2 = FileUtil.createTempFile("move", ".tmp");
    f2.delete();
    f2.deleteOnExit();
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f = createDataFile(5);
    Cursor c = new Cursor(q, f);

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
  public void testCursorRotatePrexistingSameSizeWithDelete()
      throws IOException, InterruptedException {

    // Windows rename semantics different than unix
    Assume.assumeTrue(!OSUtils.isWindowsOS());

    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    File f2 = FileUtil.createTempFile("move", ".tmp");
    f2.delete();
    f2.deleteOnExit();
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f = createDataFile(5);
    Cursor c = new Cursor(q, f);

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
  public void testCursorRotatePrexistingSameSizeWithNewModtime()
      throws IOException, InterruptedException {

    // Windows rename semantics different than unix
    Assume.assumeTrue(!OSUtils.isWindowsOS());

    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    File f2 = FileUtil.createTempFile("move", ".tmp");
    f2.delete();
    f2.deleteOnExit();
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    File f = createDataFile(5);
    Cursor c = new Cursor(q, f);

    assertTrue(c.tailBody()); // open the file
    assertTrue(c.tailBody()); // finish reading the first file
    assertEquals(5, q.size()); // should be 5 in queue.

    assertFalse(c.tailBody()); // no more to read
    assertFalse(c.tailBody()); // no more to read

    // wait a second to force a new last modified time.
    f.renameTo(f2); // move the file (should be no problem).
    Clock.sleep(1000);
    appendData(f, 5, 5);

    assertTrue(c.tailBody()); // notice new mod time and reset, file has data to read
    assertTrue(c.tailBody()); // open the new file
    assertTrue(c.tailBody()); // read new file
    assertFalse(c.tailBody()); // no more to read
    assertEquals(10, q.size());
  }

  /**
   * read, write to file, read more
   */
  @Test
  public void testCursorNewAppendPrexisting() throws IOException,
      InterruptedException {
    // normal implementation uses synchronous queue, but we use array blocking
    // queue for single threaded testing
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(10);
    File f = createDataFile(5);
    f.deleteOnExit();
    Cursor c = new Cursor(q, f);

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
    File f = FileUtil.createTempFile("appear", ".tmp");
    f.delete();
    f.deleteOnExit();
    Cursor c = new Cursor(q, f);

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
    File f = FileUtil.createTempFile("appear", ".tmp");
    f.delete();
    f.deleteOnExit();
    Cursor c = new Cursor(q, f);

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
    Cursor c = new Cursor(q, f);

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
    Cursor c1 = new Cursor(q, f1);

    File f2 = createDataFile(5);
    Cursor c2 = new Cursor(q, f2);

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

  // /////////////////////////////////////////////////////////////////////////

  /**
   * This an experiment playing with FileDescriptors.
   * 
   * This doesn't test flume per se but is a jvm operating system consistency
   * check.
   */
  @Test
  public void testFileDescriptor() throws IOException {
    File f = FileUtil.createTempFile("fdes", ".tmp");
    f.deleteOnExit();
    File f2 = FileUtil.createTempFile("fdes", ".tmp");
    f2.delete();
    f2.deleteOnExit();

    FileInputStream fis = new FileInputStream(f);

    FileDescriptor fd = fis.getFD();
    f.renameTo(f2);
    FileDescriptor fd2 = fis.getFD();

    assertEquals(fd, fd2);

    new File(f.getAbsolutePath()).createNewFile();
    FileInputStream fis2 = new FileInputStream(f);
    FileDescriptor fd3 = fis2.getFD();
    assertTrue(fd3 != fd);

  }

  /**
   * This shows that the FD semantics seem sane.
   * 
   * This is the way to do it and should be enough to make tail just about work.
   */
  @Test
  public void testFISFollowsFD() throws IOException {
    File f = FileUtil.createTempFile("fdes", ".tmp");
    f.deleteOnExit();
    File f2 = FileUtil.createTempFile("fdes", ".tmp");
    f2.delete();
    f2.deleteOnExit();

    FileOutputStream fos = new FileOutputStream(f);

    FileInputStream fis = new FileInputStream(f);
    FileChannel fc = fis.getChannel();
    ByteBuffer bb = ByteBuffer.allocate(1 * 1024 * 1024); // read a meg
    int read = fc.read(bb);
    assertEquals(-1, read); // reached EOF

    fos.write("test\n".getBytes());
    fos.flush();

    long sz = (long) fc.size();
    assertEquals(5, sz);
    read = fc.read(bb);
    assertEquals(5, read);

    byte b[] = new byte[(int) sz];
    bb.flip();

    // scan for new line, if none, copy and get more.
    bb.get(b);
    bb.flip();

    f.renameTo(f2);

    // Write more to same descriptor
    fos.write("test\n".getBytes());
    fos.flush();

    sz = (long) fc.size();
    assertEquals(10, sz);
    read = fc.read(bb);
    assertEquals(5, read);

    // Must get all read
    b = new byte[(int) read];
    bb.flip();

    bb.get(b);
    bb.flip();

    fos.write("test\n".getBytes());
    fos.flush();

    sz = (long) fc.size();
    assertEquals(15, sz);
    read = fc.read(bb);
    assertEquals(5, read);

  }

  /**
   * This test shows that open file channels follow file descriptor
   */
  @Test
  public void testChannelFollowsHandle() throws IOException {
    File f = FileUtil.createTempFile("first", ".tmp");
    f.deleteOnExit();
    File f2 = FileUtil.createTempFile("second", ".tmp");
    f2.delete();
    f2.deleteOnExit();

    RandomAccessFile raf = new RandomAccessFile(f, "r");
    FileChannel ch = raf.getChannel();

    ByteBuffer buf = ByteBuffer.allocate(4);
    FileWriter fw = new FileWriter(f);
    fw.write("this test is 20bytes");
    fw.flush();
    assertEquals(4, ch.read(buf));
    buf.flip();
    assertEquals(4, ch.read(buf));
    buf.flip();
    f.renameTo(f2);

    assertEquals(4, ch.read(buf));
    buf.flip();
    assertEquals(4, ch.read(buf));
    buf.flip();
    assertEquals(4, ch.read(buf));
    assertEquals("ytes", new String(buf.array()));
    buf.flip();
  }

  /**
   * This shows that file descriptors from the same RAF are the same, however,
   * two RAFs have different fileDescriptors. This is unfortunate because it
   * means we cannot use FileDescriptors to differentiate by inode, and limits
   * our tail implemenetation
   */
  @Test
  public void testFileDescriptorEquals() throws IOException {
    File f = FileUtil.createTempFile("first", ".tmp");
    f.deleteOnExit();
    File f2 = FileUtil.createTempFile("second", ".tmp");
    f2.delete();
    f2.deleteOnExit();

    RandomAccessFile raf = new RandomAccessFile(f, "r");
    FileDescriptor fd = raf.getFD();

    // same file
    RandomAccessFile raf2 = new RandomAccessFile(f, "r");
    FileDescriptor fd2 = raf2.getFD();

    // NOTE: This is unfortunate -- I cannot tell if both are the same file.
    assertFalse(fd.equals(fd2));
  }

  /**
   * Handle truncation. Here we write a file, read it, truncated it, and then
   * read again.
   */
  @Test
  public void testTruncate() throws IOException {
    File f = FileUtil.createTempFile("first", ".tmp");
    f.deleteOnExit();

    FileWriter fw = new FileWriter(f);
    fw.append("this is a test");
    fw.close();

    RandomAccessFile raf = new RandomAccessFile(f, "r");
    String line = raf.readLine();
    assertEquals("this is a test", line);

    RandomAccessFile raf2 = new RandomAccessFile(f, "rw");
    raf2.setLength(4);
    raf2.close();

    // read original
    String line2 = raf.readLine();
    assertEquals(null, line2);

    // NOTE: this is unfortunate. This basically shows that that if a file gets
    // truncated, the current open file handle dones't know what is going on.

    // A sane behavior here is to just assume it is a different file and reload
    // it from the beginning.
    assertEquals(14, raf.getFilePointer()); // file pointer is past end of file.
    assertEquals(4, f.length());
  }

  /**
   * Test tail file handle exhaustion. If there is a leak, this should fail.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testHandleExhaust() throws IOException, InterruptedException {

    // Windows rename semantics different than unix
    Assume.assumeTrue(!OSUtils.isWindowsOS());

    File f = FileUtil.createTempFile("tailexhaust", ".txt");
    f.deleteOnExit();
    File f2 = FileUtil.createTempFile("tailexhaust", ".txt");
    f2.deleteOnExit();
    BlockingQueue<Event> q = new ArrayBlockingQueue<Event>(100);
    Cursor c = new Cursor(q, f);

    for (int i = 0; i < 3000; i++) {
      f2.delete();

      appendData(f, i * 5, 5);

      assertTrue(c.tailBody()); // open the file

      f.renameTo(f2); // move the file (should be no problem).

      assertTrue(c.tailBody()); // finish reading the file
      assertEquals(5, q.size()); // should be 5 in queue.

      assertFalse(c.tailBody()); // No more to read
      assertEquals(5, q.size()); // should be 5 in queue.

      q.clear();
    }
    Log.info("file handles didn't leak!");
  }
}
