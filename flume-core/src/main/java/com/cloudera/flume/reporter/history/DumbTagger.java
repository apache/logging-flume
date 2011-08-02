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
package com.cloudera.flume.reporter.history;

import java.util.Date;

import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.util.Clock;

/**
 * This is a that only keeps track of dates. It is a simple way to generate
 * unique names. (Used in a history entries, in a rolling log).
 */
public class DumbTagger implements Tagger {

  Date d;

  public DumbTagger() {
    d = Clock.date();
  }

  @Override
  public Date getDate() {
    return new Date(d.getTime());
  }

  @Override
  public String getTag() {
    return "time_" + d.getTime();
  }

  @Override
  public String newTag() {
    d = Clock.date();
    return getTag();
  }

}
