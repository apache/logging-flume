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
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

/**
 * <p>This class makes the following assumptions:</p>
 * <ol>
 *   <li>The underlying file is not changing while it is being read</li>
 * </ol>
 *
 * <p>The ability to {@link #reset()} is dependent on the underlying {@link
 * PositionTracker} instance's durability semantics.</p>
 *
 * <p><strong>A note on surrogate pairs:</strong></p>
 *
 * <p>The logic for decoding surrogate pairs is as follows:
 * If no character has been decoded by a "normal" pass, and the buffer still has remaining bytes,
 * then an attempt is made to read 2 characters in one pass.
 * If it succeeds, then the first char (high surrogate) is returned;
 * the second char (low surrogate) is recorded internally,
 * and is returned at the next call to {@link #readChar()}.
 * If it fails, then it is assumed that EOF has been reached.</p>
 *
 * <p>Impacts on position, mark and reset: when a surrogate pair is decoded, the position
 * is incremented by the amount of bytes taken to decode the <em>entire</em> pair (usually, 4).
 * This is the most reasonable choice since it would not be advisable
 * to reset a stream to a position pointing to the second char in a pair of surrogates:
 * such a dangling surrogate would not be properly decoded without its counterpart.</p>
 *
 * <p>Thus the behaviour of mark and reset is as follows:</p>
 *
 * <ol>
 *   <li>If {@link #mark()} is called after a high surrogate pair has been returned by
 *   {@link #readChar()}, the marked position will be that of the character <em>following</em>
 *   the low surrogate, <em>not</em> that of the low surrogate itself.</li>
 *   <li>If {@link #reset()} is called after a high surrogate pair has been returned by
 *   {@link #readChar()}, the low surrogate is always returned by the next call to
 *   {@link #readChar()}, <em>before</em> the stream is actually reset to the last marked
 *   position.</li>
 * </ol>
 *
 * <p>This ensures that no dangling high surrogate could ever be read as long as
 * the same instance is used to read the whole pair. <strong>However, if {@link #reset()}
 * is called after a high surrogate pair has been returned by {@link #readChar()},
 * and a new instance of ResettableFileInputStream is used to resume reading,
 * then the low surrogate char will be lost,
 * resulting in a corrupted sequence of characters (dangling high surrogate).</strong>
 * This situation is hopefully extremely unlikely to happen in real life.
 * </p>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ResettableFileInputStream extends ResettableInputStream
    implements RemoteMarkable, LengthMeasurable {

  Logger logger = LoggerFactory.getLogger(ResettableFileInputStream.class);

  public static final int DEFAULT_BUF_SIZE = 16384;

  /**
   * The minimum acceptable buffer size to store bytes read
   * from the underlying file. A minimum size of 8 ensures that the
   * buffer always has enough space to contain multi-byte characters,
   * including special sequences such as surrogate pairs, Byte Order Marks, etc.
   */
  public static final int MIN_BUF_SIZE = 8;

  private final File file;
  private final PositionTracker tracker;
  private final FileInputStream in;
  private final FileChannel chan;
  private final ByteBuffer buf;
  private final CharBuffer charBuf;
  private final byte[] byteBuf;
  private final long fileSize;
  private final CharsetDecoder decoder;
  private long position;
  private long syncPosition;
  private int maxCharWidth;


  /**
   * Whether this instance holds a low surrogate character.
   */
  private boolean hasLowSurrogate = false;

  /**
   * A low surrogate character read from a surrogate pair.
   * When a surrogate pair is found, the high (first) surrogate pair
   * is returned upon a call to {@link #read()},
   * while the low (second) surrogate remains stored in memory,
   * to be returned at the next call to {@link #read()}.
   */
  private char lowSurrogate;

  /**
   *
   * @param file
   *        File to read
   *
   * @param tracker
   *        PositionTracker implementation to make offset position durable
   *
   * @throws FileNotFoundException If the file to read does not exist
   * @throws IOException If the position reported by the tracker cannot be sought
   */
  public ResettableFileInputStream(File file, PositionTracker tracker)
      throws IOException {
    this(file, tracker, DEFAULT_BUF_SIZE, Charsets.UTF_8, DecodeErrorPolicy.FAIL);
  }

  /**
   *
   * @param file
   *        File to read
   *
   * @param tracker
   *        PositionTracker implementation to make offset position durable
   *
   * @param bufSize
   *        Size of the underlying buffer used for input. If lesser than {@link #MIN_BUF_SIZE},
   *        a buffer of length {@link #MIN_BUF_SIZE} will be created instead.
   *
   * @param charset
   *        Character set used for decoding text, as necessary
   *
   * @param decodeErrorPolicy
   *        A {@link DecodeErrorPolicy} instance to determine how
   *        the decoder should behave in case of malformed input and/or
   *        unmappable character.
   *
   * @throws FileNotFoundException If the file to read does not exist
   * @throws IOException If the position reported by the tracker cannot be sought
   */
  public ResettableFileInputStream(File file, PositionTracker tracker,
      int bufSize, Charset charset, DecodeErrorPolicy decodeErrorPolicy)
      throws IOException {
    this.file = file;
    this.tracker = tracker;
    this.in = new FileInputStream(file);
    this.chan = in.getChannel();
    this.buf = ByteBuffer.allocateDirect(Math.max(bufSize, MIN_BUF_SIZE));
    buf.flip();
    this.byteBuf = new byte[1]; // single byte
    this.charBuf = CharBuffer.allocate(2); // two chars for surrogate pairs
    charBuf.flip();
    this.fileSize = file.length();
    this.decoder = charset.newDecoder();
    this.position = 0;
    this.syncPosition = 0;
    if (charset.name().startsWith("UTF-8")) {
      // some JDKs wrongly report 3 bytes max
      this.maxCharWidth = 4;
    } else if (charset.name().startsWith("UTF-16")) {
      // UTF_16BE and UTF_16LE wrongly report 2 bytes max
      this.maxCharWidth = 4;
    } else if (charset.name().startsWith("UTF-32")) {
      // UTF_32BE and UTF_32LE wrongly report 4 bytes max
      this.maxCharWidth = 8;
    } else {
      this.maxCharWidth = (int) Math.ceil(charset.newEncoder().maxBytesPerChar());
    }

    CodingErrorAction errorAction;
    switch (decodeErrorPolicy) {
      case FAIL:
        errorAction = CodingErrorAction.REPORT;
        break;
      case REPLACE:
        errorAction = CodingErrorAction.REPLACE;
        break;
      case IGNORE:
        errorAction = CodingErrorAction.IGNORE;
        break;
      default:
        throw new IllegalArgumentException(
            "Unexpected value for decode error policy: " + decodeErrorPolicy);
    }
    decoder.onMalformedInput(errorAction);
    decoder.onUnmappableCharacter(errorAction);

    seek(tracker.getPosition());
  }

  @Override
  public synchronized int read() throws IOException {
    int len = read(byteBuf, 0, 1);
    if (len == -1) {
      return -1;
    // len == 0 should never happen
    } else if (len == 0) {
      return -1;
    } else {
      return byteBuf[0] & 0xFF;
    }
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    logger.trace("read(buf, {}, {})", off, len);

    if (position >= fileSize) {
      return -1;
    }

    if (!buf.hasRemaining()) {
      refillBuf();
    }

    int rem = buf.remaining();
    if (len > rem) {
      len = rem;
    }
    buf.get(b, off, len);
    incrPosition(len, true);
    return len;
  }

  @Override
  public synchronized int readChar() throws IOException {

    // Check whether we are in the middle of a surrogate pair,
    // in which case, return the last (low surrogate) char of the pair.
    if (hasLowSurrogate) {
      hasLowSurrogate = false;
      return lowSurrogate;
    }

    // The decoder can have issues with multi-byte characters.
    // This check ensures that there are at least maxCharWidth bytes in the buffer
    // before reaching EOF.
    if (buf.remaining() < maxCharWidth) {
      buf.clear();
      buf.flip();
      refillBuf();
    }

    int start = buf.position();
    charBuf.clear();
    charBuf.limit(1);

    boolean isEndOfInput = false;
    if (position >= fileSize) {
      isEndOfInput = true;
    }

    CoderResult res = decoder.decode(buf, charBuf, isEndOfInput);
    if (res.isMalformed() || res.isUnmappable()) {
      res.throwException();
    }

    int delta = buf.position() - start;

    charBuf.flip();

    // Found a single char
    if (charBuf.hasRemaining()) {
      char c = charBuf.get();
      incrPosition(delta, true);
      return c;
    }

    // Found nothing, but the byte buffer has not been entirely consumed.
    // This situation denotes the presence of a surrogate pair
    // that can only be decoded if we have a 2-char buffer.
    if (buf.hasRemaining()) {
      charBuf.clear();
      // increase the limit to 2
      charBuf.limit(2);
      // decode 2 chars in one pass
      res = decoder.decode(buf, charBuf, isEndOfInput);
      if (res.isMalformed() || res.isUnmappable()) {
        res.throwException();
      }
      charBuf.flip();
      // Check if we successfully decoded 2 chars
      if (charBuf.remaining() == 2) {
        char highSurrogate = charBuf.get();
        // save second (low surrogate) char for later consumption
        lowSurrogate = charBuf.get();
        // Check if we really have a surrogate pair
        if (!Character.isHighSurrogate(highSurrogate) || !Character.isLowSurrogate(lowSurrogate)) {
          // This should only happen in case of bad sequences (dangling surrogate, etc.)
          logger.warn("Decoded a pair of chars, but it does not seem to be a surrogate pair: {} {}",
                      (int)highSurrogate, (int)lowSurrogate);
        }
        hasLowSurrogate = true;
        // consider the pair as a single unit and increment position normally
        delta = buf.position() - start;
        incrPosition(delta, true);
        // return the first (high surrogate) char of the pair
        return highSurrogate;
      }
    }

    // end of file
    incrPosition(delta, false);
    return -1;

  }

  private void refillBuf() throws IOException {
    buf.compact();
    chan.position(position); // ensure we read from the proper offset
    chan.read(buf);
    buf.flip();
  }

  @Override
  public void mark() throws IOException {
    tracker.storePosition(tell());
  }

  @Override
  public void markPosition(long position) throws IOException {
    tracker.storePosition(position);
  }

  @Override
  public long getMarkPosition() throws IOException {
    return tracker.getPosition();
  }

  @Override
  public void reset() throws IOException {
    seek(tracker.getPosition());
  }

  @Override
  public long length() throws IOException {
    return file.length();
  }

  @Override
  public long tell() throws IOException {
    logger.trace("Tell position: {}", syncPosition);

    return syncPosition;
  }

  @Override
  public synchronized void seek(long newPos) throws IOException {
    logger.trace("Seek to position: {}", newPos);

    // check to see if we can seek within our existing buffer
    long relativeChange = newPos - position;
    if (relativeChange == 0) return; // seek to current pos => no-op

    long newBufPos = buf.position() + relativeChange;
    if (newBufPos >= 0 && newBufPos < buf.limit()) {
      // we can reuse the read buffer
      buf.position((int)newBufPos);
    } else {
      // otherwise, we have to invalidate the read buffer
      buf.clear();
      buf.flip();
    }

    // clear decoder state
    decoder.reset();

    // perform underlying file seek
    chan.position(newPos);

    // reset position pointers
    position = syncPosition = newPos;
  }

  private void incrPosition(int incr, boolean updateSyncPosition) {
    position += incr;
    if (updateSyncPosition) {
      syncPosition = position;
    }
  }

  @Override
  public void close() throws IOException {
    tracker.close();
    in.close();
  }

}
