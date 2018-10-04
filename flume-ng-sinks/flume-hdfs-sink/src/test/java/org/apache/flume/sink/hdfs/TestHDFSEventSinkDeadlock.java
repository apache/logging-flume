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
package org.apache.flume.sink.hdfs;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.SinkProcessor;
import org.apache.flume.SinkRunner;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.sink.DefaultSinkProcessor;
import org.apache.flume.source.SequenceGeneratorSource;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestHDFSEventSinkDeadlock {

  public static void main(String... args) {
    HDFSEventSink sink = new HDFSEventSink();
    sink.setName("HDFSEventSink");

    Context context = new Context(ImmutableMap.of(
        "hdfs.path", "file:///tmp/flume-test/bucket-%t",
        "hdfs.filePrefix", "flumetest",
        "hdfs.rollInterval", "1",
        "hdfs.maxOpenFiles", "1",
        "hdfs.useLocalTimeStamp", "true"));
    Configurables.configure(sink, context);

    Channel channel = new MemoryChannel();
    Configurables.configure(channel, new Context());

    final SequenceGeneratorSource source = new SequenceGeneratorSource();
    Configurables.configure(source, new Context());

    ChannelSelector rcs = new ReplicatingChannelSelector();
    rcs.setChannels(Collections.singletonList(channel));
    source.setChannelProcessor(new ChannelProcessor(rcs));

    sink.setChannel(channel);

    channel.start();
    source.start();

    SinkProcessor sinkProcessor = new DefaultSinkProcessor();
    sinkProcessor.setSinks(Collections.singletonList(sink));
    SinkRunner sinkRunner = new SinkRunner();
    sinkRunner.setSink(sinkProcessor);
    sinkRunner.start();

    ScheduledExecutorService executor = Executors.newScheduledThreadPool(3);

    executor.execute(new Runnable() {
      @Override
      public void run() {
        int i = 0;
        while (true) {
          try {
            source.process();
            System.out.println(i++);
            if (i == 250) {
              System.out.println("No deadlock found after 250 iterations, exiting");
              System.exit(0);
            }
            Thread.sleep((long) (Math.random() * 100 + 950));
          } catch (Exception e) {
            //
          }
        }
      }
    });

    executor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long[] threadIds = bean.findDeadlockedThreads();
        if (threadIds != null) {
          System.out.println("Deadlocked threads found");
          printThreadStackTraces(threadIds);
          System.exit(1);
        }
      }
    }, 0, 1, TimeUnit.SECONDS);
  }

  private static void printThreadStackTraces(long[] threadIds) {
    Set<Long> threadIdSet = new HashSet<>(Longs.asList(threadIds));
    for (Thread th : Thread.getAllStackTraces().keySet()) {
      if (threadIdSet.contains(th.getId())) {
        System.out.println("Thread: " + th);
        for (StackTraceElement e : th.getStackTrace()) {
          System.out.println("\t" + e);
        }
        System.out.println("-----------------------------");
      }
    }
  }
}