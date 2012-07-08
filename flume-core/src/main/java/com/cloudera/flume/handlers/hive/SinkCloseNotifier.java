/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.flume.handlers.hive;

import java.io.IOException;

import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;

/**
 * This sends an arbitrary notification event to a subclass specified
 * notification handler.
 */
abstract public class SinkCloseNotifier<S extends EventSink, E> extends
    EventSinkDecorator<S> {

  public SinkCloseNotifier(S s) {
    super(s);
  }

  /**
   * Overridden to provide an notification specific data to the notification
   * handler
   */
  public abstract E getNotificationEvent();

  /**
   * Handle notification data
   */
  public abstract void notify(E e);

  @Override
  public void close() throws IOException, InterruptedException {
    super.close();
    notify(getNotificationEvent());
  }

}
