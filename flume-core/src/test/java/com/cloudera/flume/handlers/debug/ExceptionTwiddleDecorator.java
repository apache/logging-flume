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
package com.cloudera.flume.handlers.debug;

import java.io.IOException;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventSink;
import com.cloudera.flume.core.EventSinkDecorator;

/**
 * This sink lets me twiddle whether or not the next call to open/close/append
 * will fail or succeed. Great for testing failover cases.
 */
public class ExceptionTwiddleDecorator<S extends EventSink> extends
    EventSinkDecorator<S> {

  boolean appendOk = true;
  boolean closeOk = true;
  boolean openOk = true;

  public ExceptionTwiddleDecorator(S snk) {
    super(snk);
  }

  @Override
  public void append(Event e) throws IOException, InterruptedException {
    if (!appendOk) {
      throw new IOException("fail");
    }
    super.append(e);
  }

  @Override
  public void close() throws IOException, InterruptedException {
    if (!closeOk)
      throw new IOException("fail");
    super.close();
  }

  @Override
  public void open() throws IOException, InterruptedException {
    if (!openOk)
      throw new IOException("fail");
    super.open();
  }

  public boolean isAppendOk() {
    return appendOk;
  }

  public void setAppendOk(boolean appendOk) {
    this.appendOk = appendOk;
  }

  public boolean isCloseOk() {
    return closeOk;
  }

  public void setCloseOk(boolean closeOk) {
    this.closeOk = closeOk;
  }

  public boolean isOpenOk() {
    return openOk;
  }

  public void setOpenOk(boolean openOk) {
    this.openOk = openOk;
  }

}
