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

import java.io.IOException;

/** Allows for calling mark() without a seek() */
public interface RemoteMarkable {

  /**
   * Indicate that the specified position should be returned to in the case of
   * {@link Resettable#reset()} being called.
   * @throws java.io.IOException
   */
  void markPosition(long position) throws IOException;

  /**
   * Return the saved mark position without moving the mark pointer.
   * @throws IOException
   */
  long getMarkPosition() throws IOException;

}
