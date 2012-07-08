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
package com.cloudera.flume.reporter.sampler;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.flume.core.Event;
import com.cloudera.flume.core.EventImpl;
import com.cloudera.flume.handlers.rolling.Tagger;
import com.cloudera.flume.reporter.ReportEvent;
import com.cloudera.flume.reporter.aggregator.CounterSink;
import com.cloudera.flume.reporter.history.DumbTagger;
import com.cloudera.flume.reporter.history.ScheduledHistoryReporter;
import com.cloudera.util.Clock;
import com.cloudera.util.Pair;

/**
 * This tests sampler classes and integration into history reporters.
 * 
 * In all three tests, the decorator pattern is used to compose sinks. The
 * state/history capturing Counter sink builds its report based on the events
 * passed on by the respective sampler.
 * 
 * { xxxSampler => ScheduledHistoryReporter ( CounterSink ) }
 * 
 * (It used to be: schedule (sampler => counter). This change requires the
 * reservoir sampler to have a flush method.)
 * 
 */
public class TestSamplers {

  static long longwait = 10000000; // using forced rotation instead of timed

  @Test
  public void testReserviorSamplerSink() throws IOException,
      InterruptedException {
    System.out.println("Reservoir sampler");

    // rotations.
    long historyLen = 300;
    ScheduledHistoryReporter<CounterSink> hist = new ScheduledHistoryReporter<CounterSink>(
        "test", longwait, historyLen, new DumbTagger()) {

      @Override
      public CounterSink newSink(Tagger format) throws IOException {
        CounterSink count = new CounterSink("count") {
          public void append(Event e) throws IOException, InterruptedException {
            super.append(e);
            System.out.println(e); // just add a printf to the counts.
          }
        };
        return count;
      }

      @Override
      public ReportEvent getMetrics() {
        return null;
      }

      @Override
      public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
        reports.put(namePrefix + getName(), getMetrics());
      }

    };
    ReservoirSamplerDeco<ScheduledHistoryReporter<CounterSink>> samples = new ReservoirSamplerDeco<ScheduledHistoryReporter<CounterSink>>(
        hist, 10);

    samples.open();

    // do a few epochs, sending in different numbers of events.
    int[] events = { 15, 4, 234, 20 };
    for (int i = 0; i < events.length; i++) {
      for (int j = 0; j < events[i]; j++) {
        String s = "test " + i + "," + j;
        samples.append(new EventImpl(s.getBytes()));
      }
      samples.flush();
      hist.forcedRotate();
    }

    // at most 10 events should be accepted per epoch.
    int[] ans = { 10, 4, 10, 10 };
    List<Pair<Long, CounterSink>> h = hist.getHistory();
    int i = 0;
    for (Pair<Long, CounterSink> p : h) {
      long count = p.getRight().getCount();
      System.out.println(p.getLeft() + " :: " + count);
      i++;
    }

    i = 0;
    for (Pair<Long, CounterSink> p : h) {
      long count = p.getRight().getCount();
      Assert.assertEquals(ans[i], count);
      i++;
    }
  }

  @Test
  public void testSimpleIntervalSamplerSink() throws IOException,
      InterruptedException {
    System.out.println("Simple interval sampler sink");
    CounterSink count = new CounterSink("count");
    IntervalSampler<CounterSink> sink = new IntervalSampler<CounterSink>(count,
        10);
    sink.open();
    for (int i = 0; i < 30; i++) {
      sink.append(new EventImpl(("test " + i).getBytes()));
    }
    Assert.assertEquals(3, count.getCount());
    sink.close();

    // do the boundary condition.
    CounterSink count2 = new CounterSink("count");
    IntervalSampler<CounterSink> sink2 = new IntervalSampler<CounterSink>(
        count2, 10);
    sink2.open();
    for (int i = 0; i < 31; i++) {
      sink2.append(new EventImpl(("test " + i).getBytes()));
    }
    Assert.assertEquals(4, count2.getCount());
    sink2.close();
  }

  @Test
  public void testIntervalSamplerSink() throws IOException,
      InterruptedException {
    System.out.println("IntervalSamplerSink");
    Clock.resetDefault();

    long historyLen = 300;
    ScheduledHistoryReporter<CounterSink> sched = new ScheduledHistoryReporter<CounterSink>(
        "test", longwait, historyLen, new DumbTagger()) {

      @Override
      public CounterSink newSink(Tagger format) throws IOException {
        CounterSink count = new CounterSink("count") {
          public void append(Event e) throws IOException, InterruptedException {
            super.append(e);

            // just add a printf to the counts.
            System.out.println(e.getTimestamp() + " " + e);
          }
        };
        return count;
      }

      @Override
      public ReportEvent getMetrics() {
        return null;
      }

      @Override
      public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
        reports.put(namePrefix + getName(), getMetrics());
      }
    };
    IntervalSampler<ScheduledHistoryReporter<CounterSink>> hist = new IntervalSampler<ScheduledHistoryReporter<CounterSink>>(
        sched, 10);

    hist.open();

    // do a few epochs, sending in different numbers of events.
    // int[] events = { 15, 4, 234, 20 };
    int[] events = { 10, 40, 2030, 20 };
    for (int i = 0; i < events.length; i++) {
      for (int j = 0; j < events[i]; j++) {
        String s = "test " + i + "," + j;
        hist.append(new EventImpl(s.getBytes()));
      }
      sched.forcedRotate();
    }

    // should match at 0, 10, 20, 30, etc..
    int[] ans = { 1, 4, 203, 2 };
    List<Pair<Long, CounterSink>> h = sched.getHistory();
    int i = 0;
    for (Pair<Long, CounterSink> p : h) {
      long count = p.getRight().getCount();
      System.out.println(p.getLeft() + " :: " + count);
      i++;
    }

    i = 0;
    for (Pair<Long, CounterSink> p : h) {
      long count = p.getRight().getCount();
      Assert.assertEquals(ans[i], count);
      i++;
    }
  }

  @Test
  public void testProbabilitySamplerSink() throws IOException,
      InterruptedException {
    System.out.println("Probability sampler");
    long historyLen = 300;
    ScheduledHistoryReporter<CounterSink> hist = new ScheduledHistoryReporter<CounterSink>(
        "test", longwait, historyLen, new DumbTagger()) {

      @Override
      public CounterSink newSink(Tagger format) throws IOException {
        CounterSink count = new CounterSink("count") {
          public void append(Event e) throws IOException, InterruptedException {
            super.append(e);
            System.out.println(e); // just add a printf to the counts.
          }
        };
        return count;
      }

      @Override
      public ReportEvent getMetrics() {
        return null;
      }

      @Override
      public void getReports(String namePrefix, Map<String, ReportEvent> reports) {
        reports.put(namePrefix + getName(), getMetrics());
      }
    };
    ProbabilitySampler<ScheduledHistoryReporter<CounterSink>> sample = new ProbabilitySampler<ScheduledHistoryReporter<CounterSink>>(
        hist, .10, 1337);
    sample.open();

    // do a few epochs, sending in different numbers of events.
    int[] events = { 15, 4, 234, 20 };
    for (int i = 0; i < events.length; i++) {
      for (int j = 0; j < events[i]; j++) {
        String s = "test " + i + "," + j;
        sample.append(new EventImpl(s.getBytes()));
      }
      hist.forcedRotate();
    }

    // should match at 0, 10, 20, 30, etc..
    int[] ans = { 1, 0, 20, 4 };
    List<Pair<Long, CounterSink>> h = hist.getHistory();
    int i = 0;
    for (Pair<Long, CounterSink> p : h) {
      long count = p.getRight().getCount();
      System.out.println(p.getLeft() + " :: " + count);
      Assert.assertEquals(ans[i], count);
      i++;
    }
  }
}
