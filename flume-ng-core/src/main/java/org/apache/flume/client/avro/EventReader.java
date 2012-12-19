/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.client.avro;

import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * A line reader produces a stream of lines for the {@link AvroCLIClient} to
 * ingest into Flume. The stream may be finite or infinite.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface EventReader extends Closeable {

  /**
   * Get the next line associated with the input stream. If this returns
   * {@code null}, the input underlying input source is considered finished.
   * Note that this is allowed to block for indefinite amounts of time waiting
   * to generate a new line.
   */
  public Event readEvent() throws IOException;

  /**
   * Get up to {@code n} lines associated with the input stream. If this returns
   * less than n lines, the input underlying input source is considered
   * finished. Note that this is allowed to block for indefinite amounts of
   * time waiting to generate a new line.
   */
  public List<Event> readEvents(int n) throws IOException;

  /**
   * Clean-up any state associated with this reader.
   */
  public void close() throws IOException;
}
