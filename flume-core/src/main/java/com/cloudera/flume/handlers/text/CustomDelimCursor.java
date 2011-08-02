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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;

public class CustomDelimCursor extends Cursor {
  public static final Logger LOG = LoggerFactory
      .getLogger(CustomDelimCursor.class);
  final String regexDelim;
  final DelimMode delimMode;
  final Pattern pat;
  byte[] prefix = null; // only for include delimiter in next mode

  /**
   * Are deliminter sequences part of the previous, completed excluded or part
   * of the next event record.
   */
  enum DelimMode {
    INCLUDE_PREV, EXCLUDE, INCLUDE_NEXT
  };

  CustomDelimCursor(BlockingQueue<Event> sync, File f, long lastReadOffset,
      long lastFileLen, long lastMod, String regex, DelimMode h) {
    super(sync, f, lastReadOffset, lastFileLen, lastMod);
    this.regexDelim = regex;
    this.pat = Pattern.compile(regex);
    this.delimMode = h;

  }

  CustomDelimCursor(BlockingQueue<Event> sync, File f, String regex, DelimMode h) {
    super(sync, f);
    this.regexDelim = regex;
    this.pat = Pattern.compile(regex);
    this.delimMode = h;
  }

  /**
   * This is a cheat class to present a CharSequence interface backed by a
   * byte[]/ByteBuffer. This allows us to use regexes against the byte[]. Note,
   * we are puposely avoiding character encoding here.
   */
  public static class ByteBufferAsCharSequence implements CharSequence {
    ByteBuffer buf;

    ByteBufferAsCharSequence(ByteBuffer buf) {
      this.buf = buf;
    }

    @Override
    public char charAt(int arg0) {
      return (char) buf.get(arg0);
    }

    @Override
    public int length() {
      return buf.remaining();
    }

    @Override
    public CharSequence subSequence(int start, int end) {
      if (start < 0 || end < 0 || end > length() || start > end) {
        throw new IndexOutOfBoundsException("Index out of bounds start="
            + start + " end=" + end);
      }
      byte[] bs = new byte[end - start];
      for (int i = start, j = 0; i < end; i++, j++) {
        bs[j] = buf.get(i);
      }

      return new ByteBufferAsCharSequence(ByteBuffer.wrap(bs));
    }
  }

  /**
   * Note, these version is less efficient than then default '\n' searching
   * version because it calls the compact call more often and also relies on
   * regexes instead of a simple char matches.
   */
  boolean extractLines(ByteBuffer buf) throws IOException, InterruptedException {
    boolean madeProgress = false;
    int start = buf.position();
    byte[] nextPrefix = null;
    buf.mark();

    while (buf.hasRemaining()) {
      CharSequence cs = new ByteBufferAsCharSequence(buf);
      Matcher m = pat.matcher(cs);

      if (!m.find()) {
        // if not found, eat it all and go into drop mode.
        buf.reset();
        buf.compact(); // shift leftovers to front.
        return madeProgress;
      }

      int beg = m.start();
      int end = m.end();

      byte[] body = null;
      switch (delimMode) {
      case INCLUDE_PREV:
        body = new byte[end];
        buf.reset();
        buf.get(body);
        buf.mark();
        buf.compact();
        buf.flip();
        buf.mark();
        break;
      case EXCLUDE:
        body = new byte[beg];
        buf.reset();
        buf.get(body);
        buf.get(new byte[end - beg]); // toss bytes
        buf.mark();
        buf.compact();
        buf.flip();
        buf.mark();

        break;
      case INCLUDE_NEXT:
        // special case
        body = new byte[beg];
        buf.get(body);
        nextPrefix = new byte[end - beg]; // keep prefix
        buf.get(nextPrefix);
        buf.mark();
        buf.compact();
        buf.flip();
        buf.mark();
        break;

      }

      if (prefix != null) {
        byte[] body2 = new byte[prefix.length + body.length];
        int i = 0;
        for (; i < prefix.length; i++) {
          body2[i] = prefix[i];
        }
        for (int j = 0; j < body.length; j++, i++) {
          body2[i] = body[j];
        }
        body = body2;
      }
      prefix = nextPrefix;
      nextPrefix = null;

      Event e = new EventImpl(body);
      e.set(TailSource.A_TAILSRCFILE, file.getName().getBytes());
      sync.put(e);
      madeProgress = true;

    }

    // rewind for any left overs
    buf.reset();
    buf.compact(); // shift leftovers to front.
    return madeProgress;
  }

  /**
   * Flush any buffering the cursor has done. If the buffer does not end with
   * the proper delimiter, the remainder will get turned into a new event.
   * 
   * This assumes that any remainders in buf are a single event -- this
   * corresponds to the last lines of files that do not end with the proper
   * delimiter.
   * 
   * Also, prefix needs to be attached.
   */
  void flush() throws InterruptedException {
    if (raf != null) {
      try {
        raf.close(); // release handles
      } catch (IOException e) {
        LOG.error("problem closing file " + e.getMessage(), e);
      }
    }

    buf.flip(); // buffer consume mode
    int remaining = buf.remaining();
    if (remaining > 0) {
      byte[] body = new byte[remaining];
      buf.get(body, 0, remaining); // read all data

      // include prefix if present.
      if (prefix != null) {
        byte[] body2 = new byte[prefix.length + body.length];
        int i = 0;
        for (; i < prefix.length; i++) {
          body2[i] = prefix[i];
        }
        for (int j = 0; j < body.length; j++, i++) {
          body2[i] = body[j];
        }
        body = body2;
      }

      Event e = new EventImpl(body);
      e.set(TailSource.A_TAILSRCFILE, file.getName().getBytes());
      try {
        sync.put(e);
      } catch (InterruptedException e1) {
        LOG.error("interruptedException! " + e1.getMessage(), e1);
        throw e1;
      }
    }
    in = null;
    buf.clear();
  }
}
