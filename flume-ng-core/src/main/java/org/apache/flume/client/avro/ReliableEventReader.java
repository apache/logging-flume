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
package org.apache.flume.client.avro;

import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;

import java.io.IOException;

/**
 * A reliable event reader.
 * Clients must call commit() after each read operation, otherwise the
 * implementation must reset its internal buffers and return the same events
 * as it did previously.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ReliableEventReader extends EventReader {

  /**
   * Indicate to the implementation that the previously-returned events have
   * been successfully processed and committed.
   * @throws IOException
   */
  public void commit() throws IOException;

}
