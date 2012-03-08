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

package org.apache.flume;

import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.source.EventDrivenSourceRunner;
import org.apache.flume.source.PollableSourceRunner;

/**
 * A source runner controls how a source is driven.
 *
 * This is an abstract class used for instantiating derived classes.
 */
abstract public class SourceRunner implements LifecycleAware {

  private Source source;

  /**
   * Static factory method to instantiate a source runner implementation that
   * corresponds to the type of {@link Source} specified.
   *
   * @param source The source to run
   * @return A runner that can run the specified source
   * @throws IllegalArgumentException if the specified source does not implement
   * a supported derived interface of {@link SourceRunner}.
   */
  public static SourceRunner forSource(Source source) {
    SourceRunner runner = null;

    if (source instanceof PollableSource) {
      runner = new PollableSourceRunner();
      ((PollableSourceRunner) runner).setSource((PollableSource) source);
    } else if (source instanceof EventDrivenSource) {
      runner = new EventDrivenSourceRunner();
      ((EventDrivenSourceRunner) runner).setSource((EventDrivenSource) source);
    } else {
      throw new IllegalArgumentException("No known runner type for source "
          + source);
    }

    return runner;
  }

  public Source getSource() {
    return source;
  }

  public void setSource(Source source) {
    this.source = source;
  }

}
