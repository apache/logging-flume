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

import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;

import java.io.Closeable;
import java.io.IOException;

/**
 * <p> This abstract class defines an interface on top of InputStream for which
 * the stream may be <code>mark</code>ed and <code>reset</code> with no limit to
 * the number of bytes which may have been read between the calls.
 *
 * <p> Any implementation of this interface guarantees that the mark position
 * will not be invalidated by reading any number of bytes.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface ResettableInputStream extends Resettable, Closeable {

  /**
   * Read a single byte of data from the stream.
   * @return the next byte of data, or {@code -1} if the end of the stream has
   * been reached.
   * @throws IOException
   */
  public int read() throws IOException;

  /**
   * Read multiple bytes of data from the stream.
   * @param b   the buffer into which the data is read.
   * @param off Offset into the array {@code b} at which the data is written.
   * @param len the maximum number of bytes to read.
   * @return the total number of bytes read into the buffer, or {@code -1} if
   * the end of the stream has been reached.
   * @throws IOException
   */
  public int read(byte[] b, int off, int len) throws IOException;

  /**
   * <p>Read a single character.
   *
   * <p>Note that this may lead to returning only one character in a 2-char
   * surrogate pair sequence. When this happens, the underlying implementation
   * should never persist a mark between two chars of a two-char surrogate
   * pair sequence.
   *
   * @return The character read, as an integer in the range 0 to 65535
   *         (0x00-0xffff), or -1 if the end of the stream has been reached
   * @throws IOException
   */
  public int readChar() throws IOException;

  /**
   * Marks the current position in this input stream. A subsequent call to the
   * <code>reset</code> method repositions this stream at the last marked
   * position so that subsequent reads re-read the same bytes.
   *
   * <p> Marking a closed stream should not have any effect on the stream.
   *
   * @throws IOException If there is an error while setting the mark position.
   *
   * @see java.io.InputStream#mark(int)
   * @see java.io.InputStream#reset()
   */
  @Override
  public void mark() throws IOException;

  /**
   * Reset stream position to that set by {@link #mark()}
   * @throws IOException
   */
  @Override
  public void reset() throws IOException;

  @Override
  public void close() throws IOException;

}
