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
package com.cloudera.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import junit.framework.TestCase;

/**
 * This is a sandbox to test the semantics of ByteBuffer. This will provide
 * insight into how/if to use them them in when converting the events away from
 * <String,byte[]>'s
 */
public class TestByteBufferSemantics extends TestCase {

  public void testToArray() {
    ByteBuffer buf = ByteBuffer.allocate(100);
    buf.putInt(0xdead);
    buf.putInt(0xbeef);

    // returns the backing array -- the entire thing regardless of how much
    // of it is filled.
    assertEquals(100, buf.array().length);
    assertEquals(100, buf.capacity()); // same as array().length
    assertEquals(92, buf.remaining());
    assertEquals(8, buf.position());

    // byte offset for these.
    assertEquals(0xdead, buf.getInt(0));
    assertEquals(0xbeef, buf.getInt(4));

    // didn't go anywhere. whoa!
    assertEquals(100, buf.array().length);
    assertEquals(100, buf.capacity()); // same as array().length
    assertEquals(92, buf.remaining());
    assertEquals(8, buf.position());

    buf.flip();
    assertEquals(0xdead, buf.getInt());
    assertEquals(0xbeef, buf.getInt());
    // still in the same place
    assertEquals(0xdead, buf.getInt(0));
    assertEquals(0xbeef, buf.getInt(4));

    assertEquals(100, buf.array().length);
    assertEquals(100, buf.capacity()); // same as array().length
    assertEquals(0, buf.remaining());
    assertEquals(8, buf.position());

    buf.flip();
    assertEquals(100, buf.array().length);
    assertEquals(100, buf.capacity()); // same as array().length
    assertEquals(8, buf.remaining());
    assertEquals(0, buf.position());

    buf.clear(); // just resets pointers
    assertEquals(100, buf.array().length);
    assertEquals(100, buf.capacity()); // same as array().length
    assertEquals(100, buf.remaining());
    assertEquals(0, buf.position());
    // note: still not erased.
    assertEquals(0xdead, buf.getInt(0));
    assertEquals(0xbeef, buf.getInt(4));

    // this overwrites.
    buf.putInt(0xf00d);
    buf.putInt(0xcafe);
    assertEquals(0xf00d, buf.getInt(0));
    assertEquals(0xcafe, buf.getInt(4));

  }

  public void testBuffers() throws IOException {
    byte[] bs = new byte[100];
    for (int i = 0; i < bs.length; i++) {
      bs[i] = (byte) i;
    }
    InputStream is = new ByteArrayInputStream(bs);
    ReadableByteChannel rch = Channels.newChannel(is);

    ByteBuffer buf = ByteBuffer.allocate(100);
    buf.limit(8);
    System.out.println("artificially set limit to :" + buf.limit());
    rch.read(buf);
    assertEquals(0, buf.get(0));
    assertEquals(1, buf.get(1));
    assertEquals(7, buf.get(7));
    buf.clear();

    System.out.println("after clear limit is :" + buf.limit());
    assertEquals(100, buf.limit());

    // reset limit to 8.
    buf.limit(8);
    // read the next 8 bytes
    rch.read(buf);
    assertEquals(8, buf.get(0));
    assertEquals(9, buf.get(1));
    assertEquals(15, buf.get(7));

    // now can read 16
    System.out.println("limit was :" + buf.limit());
    buf.limit(buf.limit() + 8);
    System.out.println("limit now bumped up to :" + buf.limit());
    rch.read(buf);
    // still same as before
    assertEquals(8, buf.get(0));
    assertEquals(9, buf.get(1));
    assertEquals(15, buf.get(7));
    // but now new data is read as well!
    assertEquals(16, buf.get(8));
    assertEquals(17, buf.get(9));
    assertEquals(23, buf.get(15));

  }

}
