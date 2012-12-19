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

import java.io.OutputStream;
import org.apache.flume.Context;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;

/**
 * <p/>A class that is able to both serialize and deserialize events.
 * <p/>Implementing this interface does not simply mean that a class has both
 * serialization and deserialization capability. By implementing this
 * interface, implementations guarantee that they can convert a serialized
 * event back to a deserialized event, and back to a serialized event again
 * without any data loss.
 * <p/>That guarantee allows Flume to "replay" partial file writes and restore
 * output files that may have been damaged in a system crash. At the time of
 * this writing, support for this functionality is still lacking.
 */
@InterfaceAudience.LimitedPrivate("future public api")
@InterfaceStability.Unstable
public interface EventSerDe extends EventSerializer, EventDeserializer {

  /**
   * Knows how to construct this serde.<br/>
   * <b>Note: Implementations MUST provide a public a no-arg constructor.</b>
   */
  public interface Builder {
    public EventSerDe build(Context context, ResettableInputStream in, OutputStream out);
  }

}
