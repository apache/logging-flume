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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;

/**
 * Establishes a contract for reading events stored in arbitrary formats from
 * reliable, resettable streams.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface EventDeserializer extends Resettable, Closeable {

  /**
   * Read a single event from the underlying stream.
   * @return Deserialized event or {@code null} if no events could be read.
   * @throws IOException
   * @see #mark()
   * @see #reset()
   */
  public Event readEvent() throws IOException;

  /**
   * Read a batch of events from the underlying stream.
   * @param numEvents Maximum number of events to return.
   * @return List of read events, or empty list if no events could be read.
   * @throws IOException
   * @see #mark()
   * @see #reset()
   */
  public List<Event> readEvents(int numEvents) throws IOException;

  /**
   * Marks the underlying input stream, indicating that the events previously
   * returned by this EventDeserializer have been successfully committed.
   * @throws IOException
   * @see #reset()
   */
  @Override
  public void mark() throws IOException;

  /**
   * Resets the underlying input stream to the last known mark (or beginning
   * of the stream if {@link #mark()} was never previously called. This should
   * be done in the case of inability to commit previously-deserialized events.
   * @throws IOException
   * @see #mark()
   */
  @Override
  public void reset() throws IOException;

  /**
   * Calls {@link #reset()} on the stream and then closes it.
   * In the case of successful completion of event consumption,
   * {@link #mark()} MUST be called before {@code close()}.
   * @throws IOException
   * @see #mark()
   * @see #reset()
   */
  @Override
  public void close() throws IOException;

  /**
   * Knows how to construct this deserializer.<br/>
   * <b>Note: Implementations MUST provide a public a no-arg constructor.</b>
   */
  public interface Builder {
    public EventDeserializer build(Context context, ResettableInputStream in);
  }

}
