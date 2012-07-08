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
package com.cloudera.flume.reporter.builder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.arabidopsis.ahocorasick.AhoCorasick;

import com.cloudera.flume.reporter.histogram.MultiGrepReporterSink;

/**
 * This builds a aho multi string search reporter from a text file. Each line is
 * a string that will be searched for. (no new lines allowed).
 */
public class MultiGrepReporterBuilder extends
    ReporterBuilder<MultiGrepReporterSink<String>> {

  final String fname;
  final String reportName; // TODO (jon) put name into multi grep spec file.

  public MultiGrepReporterBuilder(String f) {
    this.fname = f;
    reportName = "multi grep";
  }

  public MultiGrepReporterBuilder(String f, String repname) {
    this.fname = f;
    this.reportName = repname;
  }

  @Override
  public Collection<MultiGrepReporterSink<String>> load() throws IOException {
    List<MultiGrepReporterSink<String>> l = new ArrayList<MultiGrepReporterSink<String>>();
    RandomAccessFile raf = new RandomAccessFile(fname, "r");

    // build the aho from the file.
    AhoCorasick<String> aho = new AhoCorasick<String>();
    String s;
    while ((s = raf.readLine()) != null) {
      if (s.length() == 0) {
        // skip 0 length strings
        continue;
      }
      aho.add(s.getBytes(), s);
    }

    MultiGrepReporterSink<String> snk = new MultiGrepReporterSink<String>(
        reportName, aho);
    l.add(snk);
    
    return l;
  }
}
