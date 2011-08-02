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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.Clock;

/**
 * This tests on NIO semantics for file io in order to find out if is sufficient
 * for implementing tail, and to test sub functions used in tail.
 */
public class TestFileNIO {
  public static final Logger LOG = LoggerFactory.getLogger(TestFileNIO.class);

  /**
   * if sleep = 0 there there is no pause between writes.
   */
  public CountDownLatch slowWrite(File f, final int sleep) throws IOException {
    final FileOutputStream fos = new FileOutputStream(f);
    final FileChannel out = fos.getChannel();
    final CountDownLatch done = new CountDownLatch(1);

    Thread t = new Thread() {
      public void run() {
        try {
          for (int i = 0; i < 100; i++) {
            ByteBuffer buf = ByteBuffer.wrap(("test " + i + "\n").getBytes());
            out.write(buf);
            if (sleep > 0) {
              Clock.sleep(sleep);
            }
          }
          fos.close();
          done.countDown();
        } catch (Exception e) {
          // failure and would cause countdown timeout
          LOG.error("Exception when running thread", e);
        }
      }
    };
    t.start();
    return done;
  }

  @Test
  public void testRenameSemantics() throws IOException {
    File f1 = File.createTempFile("moved", "");
    f1.delete();
    f1.deleteOnExit();
    File f2 = File.createTempFile("orig", "");
    f2.deleteOnExit();

    f2.renameTo(f1);

    LOG.info("f1 = " + f1.getAbsolutePath() + " exists " + f1.exists());
    LOG.info("f2 = " + f2.getAbsolutePath() + " exists " + f2.exists());
  }

  /**
   * This shows that the semantics of a buffer is
   * 
   * write -> (remaining=write space) ->flip -> (remaining=read space) -> read->
   * clear ->
   * 
   * write -> flip -> compact ->
   * 
   * write -> flip -> read
   * 
   */
  @Test
  public void remainingSemantics() {
    ByteBuffer buf = ByteBuffer.allocate(1000);
    // write mode
    assertEquals(1000, buf.remaining());

    // read mode.
    buf.flip();
    assertEquals(0, buf.remaining());

    // write mode
    buf.clear();
    assertEquals(1000, buf.remaining());
    byte[] bs = "this is a test".getBytes();
    buf.put(bs);

    // read mode
    buf.flip();
    assertEquals(bs.length, buf.remaining());

    // compact puts it back into write mode.
    buf.compact();
    buf.put("test2".getBytes());
    buf.flip();
    assertEquals(bs.length + 5, buf.remaining());
  }

  @Test
  public void testFileChannel() throws IOException, InterruptedException {
    File f = File.createTempFile("test", ".test");
    f.deleteOnExit();

    CountDownLatch done = slowWrite(f, 20);
    final FileInputStream fis = new FileInputStream(f);
    final FileChannel in = fis.getChannel();

    ByteBuffer buf = ByteBuffer.allocate(3);
    int loops = 0;
    int read = 0;
    while (!done.await(5, TimeUnit.MILLISECONDS)) {
      int rd = in.read(buf);
      read += (rd < 0 ? 0 : rd); // rd == -1 if at end of stream.
      buf.flip();
      byte[] arr = new byte[buf.remaining()];
      buf.get(arr);
      String s = new String(arr);
      System.out.print(s);
      buf.clear();
      loops++;
    }
    // interesting, the read bytes is not consistent.
    LOG.info("read " + read + " bytes in " + loops + " iterations");
    in.close();
    assertEquals(790, read);
  }

  /**
   * This shows that we need to do some secondary parsing because files using
   * file channels read in large chunks.
   */
  @Test
  public void testFileChannelBlockRead() throws IOException,
      InterruptedException {
    File f = File.createTempFile("test", ".test");
    f.deleteOnExit();

    CountDownLatch done = slowWrite(f, 0);
    final FileInputStream fis = new FileInputStream(f);
    final FileChannel in = fis.getChannel();

    ByteBuffer buf = ByteBuffer.allocate(1000);
    Clock.sleep(500);

    int loops = 0;
    int read = 0;
    do {
      int rd = in.read(buf);
      read += (rd < 0 ? 0 : rd); // rd == -1 if at end of stream.
      buf.flip();
      byte[] arr = new byte[buf.remaining()];
      buf.get(arr);
      String s = new String(arr);
      System.out.print(s);
      buf.clear();
      loops++;
    } while (!done.await(5, TimeUnit.MILLISECONDS));
    // interesting, the read bytes is not consistent.
    LOG.info("read " + read + " bytes in " + loops + " iterations");
    in.close();
    assertEquals(790, read);
  }

  @Test
  public void testFileChannelByLine() throws IOException, InterruptedException {
    File f = File.createTempFile("test", ".test");
    f.deleteOnExit();

    CountDownLatch done = slowWrite(f, 0);
    final FileInputStream fis = new FileInputStream(f);
    final FileChannel in = fis.getChannel();

    ByteBuffer buf = ByteBuffer.allocate(1000);
    Clock.sleep(500);

    int loops = 0;
    int read = 0;
    do {
      int rd = in.read(buf);
      read += (rd < 0 ? 0 : rd); // rd == -1 if at end of stream.
      buf.flip();

      int start = buf.position();
      buf.mark();
      while (buf.hasRemaining()) {
        byte b = buf.get();
        if (b == '\n') {
          int end = buf.position();
          int sz = end - start;
          byte[] body = new byte[sz];
          buf.reset(); // go back to mark
          buf.get(body, 0, sz - 1); // read data
          buf.get(); // read '\n'
          buf.mark(); // new mark.
          start = buf.position();
          String s = new String(body);
          LOG.info("=> " + s);
        }
      }

      buf.clear();
      loops++;
    } while (!done.await(5, TimeUnit.MILLISECONDS));
    // interesting, the read bytes is not consistent.
    LOG.info("read " + read + " bytes in " + loops + " iterations");
    in.close();
    assertEquals(790, read);
  }

  /**
   * This will break if an individual entry size is > the allocated buffer.
   * 
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testFileChannelByLineWithBreaks() throws IOException,
      InterruptedException {
    File f = File.createTempFile("test", ".test");
    f.deleteOnExit();

    CountDownLatch done = slowWrite(f, 0);
    final FileInputStream fis = new FileInputStream(f);
    final FileChannel in = fis.getChannel();

    ByteBuffer buf = ByteBuffer.allocate(20);
    Clock.sleep(500);

    int loops = 0;
    int read = 0;
    do {
      int rd = in.read(buf);
      read += (rd < 0 ? 0 : rd); // rd == -1 if at end of stream.
      buf.flip();

      int start = buf.position();
      buf.mark();
      while (buf.hasRemaining()) {
        byte b = buf.get();
        if (b == '\n') {
          int end = buf.position();
          int sz = end - start;
          byte[] body = new byte[sz];
          buf.reset(); // go back to mark
          buf.get(body, 0, sz - 1); // read data
          buf.get(); // read '\n'
          buf.mark(); // new mark.
          start = buf.position();
          String s = new String(body);
          LOG.info("=> " + s);
        }
      }

      // rewind for any left overs
      buf.reset();
      buf.compact(); // shift leftovers to front.
      loops++;
    } while (read < in.size());
    // interesting, the read bytes is not consistent.
    LOG.info("read " + read + " bytes in " + loops + " iterations");
    in.close();
    assertEquals(790, read);
  }

  @Test
  public void testFileChannelByLineWithBreaksWhileWriting() throws IOException,
      InterruptedException {
    File f = File.createTempFile("test", ".test");
    f.deleteOnExit();

    CountDownLatch done = slowWrite(f, 100);
    final FileInputStream fis = new FileInputStream(f);
    final FileChannel in = fis.getChannel();

    ByteBuffer buf = ByteBuffer.allocate(10);
    Clock.sleep(500);

    int loops = 0;
    int read = 0;
    do {
      int rd = in.read(buf);
      read += (rd < 0 ? 0 : rd); // rd == -1 if at end of stream.
      buf.flip();

      int start = buf.position();
      buf.mark();
      while (buf.hasRemaining()) {
        byte b = buf.get();
        if (b == '\n') {
          int end = buf.position();
          int sz = end - start;
          byte[] body = new byte[sz];
          buf.reset(); // go back to mark
          buf.get(body, 0, sz - 1); // read data
          buf.get(); // read '\n'
          buf.mark(); // new mark.
          start = buf.position();
          String s = new String(body);
          LOG.info("=> " + s);
        }
      }

      // rewind for any left overs
      buf.reset();
      buf.compact(); // shift leftovers to front.
      loops++;
    } while (!(done.await(10, TimeUnit.MILLISECONDS) && read == in.size()));
    LOG.info("read " + read + " bytes in " + loops + " iterations");
    in.close();
    assertEquals(790, read);
  }

  @Test
  public void testInterruptFileChannelRead() throws IOException,
      InterruptedException {
    File f = File.createTempFile("test", ".test");
    f.deleteOnExit();

    final CountDownLatch writeDone = slowWrite(f, 0);
    final CountDownLatch readDone = new CountDownLatch(1);
    final FileInputStream fis = new FileInputStream(f);
    final FileChannel in = fis.getChannel();

    Thread t = new Thread() {
      public void run() {

        ByteBuffer buf = ByteBuffer.allocate(10);

        int loops = 0;
        int read = 0;
        try {
          do {
            Clock.sleep(100);
            int rd = in.read(buf);
            read += (rd < 0 ? 0 : rd); // rd == -1 if at end of stream.
            buf.flip();

            int start = buf.position();
            buf.mark();
            while (buf.hasRemaining()) {
              byte b = buf.get();
              if (b == '\n') {
                int end = buf.position();
                int sz = end - start;
                byte[] body = new byte[sz];
                buf.reset(); // go back to mark
                buf.get(body, 0, sz - 1); // read data
                buf.get(); // read '\n'
                buf.mark(); // new mark.
                start = buf.position();
                String s = new String(body);
                LOG.info("=> " + s);
              }
            }

            // rewind for any left overs
            buf.reset();
            buf.compact(); // shift leftovers to front.
            loops++;
          } while (!(writeDone.await(10, TimeUnit.MILLISECONDS) && read == in
              .size()));

          LOG.info("read " + read + " bytes in " + loops + " iterations");
          assertEquals(790, read);
          readDone.countDown();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    };

    t.start();
    // another thread forces the file closed and should cause the reader to exit
    // prematurely.
    in.close();
    assertFalse(readDone.await(1, TimeUnit.SECONDS));
  }
}
