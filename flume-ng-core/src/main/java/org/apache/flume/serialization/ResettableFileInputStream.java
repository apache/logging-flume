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
public class ResettableFileInputStream extends NonSyncResettableFileInputStream {

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
    super(file, tracker, DEFAULT_BUF_SIZE, Charsets.UTF_8, DecodeErrorPolicy.FAIL);
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
    super(file, tracker, bufSize, charset, decodeErrorPolicy);
  }

  @Override
  public synchronized int read() throws IOException {
    return super.read();
  }

  @Override
  public synchronized int read(byte[] b, int off, int len) throws IOException {
    return super.read(b, off, len);
  }

  @Override
  public synchronized int readChar() throws IOException {
    return super.readChar();
  }
}
