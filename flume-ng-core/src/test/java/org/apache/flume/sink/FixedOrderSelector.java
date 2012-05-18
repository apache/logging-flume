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
package org.apache.flume.sink;

import java.util.Iterator;

import org.apache.flume.Context;
import org.apache.flume.Sink;

/**
 * A test selector that always returns the iteration order of specified
 * sinks for testing purposes. This selector expects that the configuration
 * key {@value #SET_ME} is specified with a non-null value.
 */
public class FixedOrderSelector extends AbstractSinkSelector {

  public static final String SET_ME = "setme";

  @Override
  public Iterator<Sink> createSinkIterator() {
    return getSinks().iterator();
  }

  @Override
  public void configure(Context context) {
    super.configure(context);

    if (context.getString(SET_ME) == null) {
      throw new RuntimeException("config key " + SET_ME + " not specified");
    }
  }
}
