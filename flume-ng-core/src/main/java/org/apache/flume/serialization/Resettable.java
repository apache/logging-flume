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

import java.io.IOException;

/**
 * Defines an API for objects that can be mark()ed and reset() on arbitrary
 * boundaries. Any implementation that has a limited buffer for the mark(),
 * like {@link java.io.InputStream}, must not implement Resettable.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface Resettable {

  /**
   * Indicate that the current position should be returned to in the case of
   * {@link #reset()} being called.
   * @throws IOException
   */
  void mark() throws IOException;

  /**
   * Return to the last marked position, or the beginning of the stream if
   * {@link #mark()} has never been called.
   * @throws IOException
   */
  void reset() throws IOException;
}
