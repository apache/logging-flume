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

package org.apache.flume.sink.kite;


/**
 * A non-recoverable error trying to deliver the event.
 * 
 * Non-recoverable event delivery failures include:
 * 
 * 1. Error parsing the event body thrown from the {@link EntityParser}
 * 2. A schema mismatch between the schema of an event and the schema of the
 *    destination dataset.
 * 3. A missing schema from the Event header when using the
 *    {@link AvroEntityParser}.
 */
public class NonRecoverableEventException extends Exception {

  private static final long serialVersionUID = 3485151222482254285L;

  public NonRecoverableEventException() {
    super();
  }

  public NonRecoverableEventException(String message) {
    super(message);
  }

  public NonRecoverableEventException(String message, Throwable t) {
    super(message, t);
  }

  public NonRecoverableEventException(Throwable t) {
    super(t);
  }

}
