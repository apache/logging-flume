/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.apache.flume.spring.boot.config;

import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.SinkRunner;
import org.apache.flume.Source;
import org.apache.flume.SourceRunner;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurables;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 */
public abstract class AbstractFlumeConfiguration {

  protected <T extends Channel> T configureChannel(final String name, final Class<T> clazz,
                                                   final Map<String, String> params) {
    T channel;
    try {
      channel = clazz.newInstance();
    } catch (Exception ex) {
      throw new FlumeException("Unable to create channel " + name, ex);
    }
    channel.setName(name);
    Configurables.configure(channel, createContext(params));
    return channel;
  }

  protected <T extends Source> SourceRunner configureSource(final String name, final Class<T> clazz,
      final ChannelSelector selector, final Map<String, String> params) {
    T source;
    try {
      source = clazz.newInstance();
    } catch (Exception ex) {
      throw new FlumeException("Unable to create source " + name, ex);
    }
    source.setName(name);
    Configurables.configure(source, createContext(params));
    source.setChannelProcessor(new ChannelProcessor(selector));
    return SourceRunner.forSource(source);
  }

  protected <T extends Source> SourceRunner configureSource(final T source,
      final ChannelSelector selector, final Map<String, String> params) {
    source.setChannelProcessor(new ChannelProcessor(selector));
    return SourceRunner.forSource(source);
  }

  protected <T extends SinkProcessor> T configureSinkProcessor(final Map<String, String> params,
      final Class<T> clazz, final List<Sink> sinks) {
    T processor;
    try {
      processor = clazz.newInstance();
    } catch (Exception ex) {
      throw new FlumeException("Unable to create SinkProcessor of type: " + clazz.getName(), ex);
    }
    processor.setSinks(sinks);
    Configurables.configure(processor, createContext(params));
    return processor;
  }


  protected SinkRunner createSinkRunner(SinkProcessor sinkProcessor) {
    SinkRunner runner = new SinkRunner(sinkProcessor);
    runner.setSink(sinkProcessor);
    return runner;
  }

  protected <T extends Sink> Sink configureSink(final String name, final Class<T> sinkClazz,
                                                final Channel channel,
                                                final Map<String, String> params) {
    T sink;
    try {
      sink = sinkClazz.newInstance();
    } catch (Exception ex) {
      throw new FlumeException("Unable to create sink " + name, ex);
    }
    sink.setName(name);
    Configurables.configure(sink, createContext(params));
    sink.setChannel(channel);
    return sink;
  }

  protected ChannelSelector createChannelSelector(Class<? extends ChannelSelector> clazz,
                                                  Map<String, String> params) {
    ChannelSelector selector;
    try {
      selector = clazz.newInstance();
    } catch (Exception ex) {
      throw new FlumeException("Unable to create channel selector " + clazz.getName(), ex);
    }
    Configurables.configure(selector, createContext(params));
    return selector;
  }

  /**
   * Creates a List from a Varargs array.
   *
   * @param items The items to add to the list.
   * @param <T>   The type of objects in the List.
   * @return a List containing the supplied items.
   */
  @SafeVarargs
  protected final <T> List<T> listOf(T... items) {
    return Arrays.asList(items);
  }

  private static Context createContext(Map<String, String> map) {
    return map != null ? new Context(map) : new Context();
  }
}
