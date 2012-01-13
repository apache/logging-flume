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
package org.apache.flume.conf.file;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Sink;
import org.apache.flume.SinkRunner;
import org.apache.flume.Source;
import org.apache.flume.SourceRunner;
import org.apache.flume.conf.Configurables;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonFileConfigurationProvider
    extends AbstractFileConfigurationProvider {

  private final Logger logger = LoggerFactory
      .getLogger(JsonFileConfigurationProvider.class);

  private void loadSources(SimpleNodeConfiguration conf,
      List<Map<String, Object>> defs) throws InstantiationException {

    logger.debug("Loading sources");

    for (Map<String, Object> sourceDef : defs) {
      logger.debug("source:{}", sourceDef);

      if (sourceDef.containsKey("type")) {
        Source source =
            getSourceFactory().create(
                (String) sourceDef.get("name"),
                (String) sourceDef.get("type"));

        //String channelList = sourceDef.

        List<String> channelNames = (List<String>) sourceDef.get("channels");

        List<Channel> channels = new ArrayList<Channel>();

        for (String chName : channelNames) {
          channels.add(conf.getChannels().get(chName));
        }

        Context context = new Context();
        context.setParameters(sourceDef);

        Configurables.configure(source, context);

        if (channels.size() > 0) {
          source.setChannels(channels);
        } else {
          logger.warn(
              "No channel named {} - source:{} is likely non-functional.",
              source, sourceDef.get("channel"));
        }

        conf.getSourceRunners().put((String) sourceDef.get("name"),
            SourceRunner.forSource(source));
      } else {
        throw new IllegalArgumentException("Illegal source definition:"
            + sourceDef + " - Missing type.");
      }
    }
  }

  private void loadSinks(SimpleNodeConfiguration conf,
      List<Map<String, Object>> defs) throws InstantiationException {

    logger.debug("Loading sinks");

    for (Map<String, Object> sinkDef : defs) {
      logger.debug("sink:{}", sinkDef);

      if (sinkDef.containsKey("type")) {
        Sink sink =
            getSinkFactory().create(
                (String) sinkDef.get("name"),
                (String) sinkDef.get("type"));
        Channel channel = conf.getChannels().get(sinkDef.get("channel"));

        Context context = new Context();
        context.setParameters(sinkDef);

        Configurables.configure(sink, context);

        if (channel != null) {
          sink.setChannel(channel);
        } else {
          logger.warn(
              "No channel named {} - sink:{} is likely non-functional.", sink,
              sinkDef.get("channel"));
        }

        conf.getSinkRunners().put((String) sinkDef.get("name"),
            SinkRunner.forSink(sink));
      }
    }
  }

  private void loadChannels(SimpleNodeConfiguration conf,
      List<Map<String, Object>> defs) throws InstantiationException {

    logger.debug("Loading channels");

    for (Map<String, Object> channelDef : defs) {
      logger.debug("channel:{}", channelDef);

      if (channelDef.containsKey("type")) {
        Channel channel = getChannelFactory().create(
                (String) channelDef.get("name"),
                (String) channelDef.get("type"));

        Context context = new Context();
        context.setParameters(channelDef);

        Configurables.configure(channel, context);

        conf.getChannels().put((String) channelDef.get("name"), channel);
      }
    }
  }

  protected synchronized void load() {
    SimpleNodeConfiguration flumeConf = new SimpleNodeConfiguration();
    ObjectMapper mapper = new ObjectMapper();
    File file = getFile();

    try {
      @SuppressWarnings("unchecked")
      Map<String, Map<String, List<Map<String, Object>>>> tree = mapper
          .readValue(file, Map.class);

      for (Entry<String, Map<String, List<Map<String, Object>>>> host : tree
          .entrySet()) {

        logger.debug("host:{}", host);

        /*
         * NB: Because load{Sources,Sinks} wire up dependencies (i.e. channels),
         * loadChannels must always be executed first.
         */
        loadChannels(flumeConf, host.getValue().get("channels"));
        loadSources(flumeConf, host.getValue().get("sources"));
        loadSinks(flumeConf, host.getValue().get("sinks"));
      }

      logger.debug("Loaded conf:{}", flumeConf);

      getConfigurationAware().onNodeConfigurationChanged(flumeConf);
    } catch (JsonParseException e) {
      logger.error("Unable to parse json file:" + file + " Exception follows.",
          e);
    } catch (IOException e) {
      logger.error("Unable to parse json file:" + file + " Exception follows.",
          e);
    } catch (InstantiationException e) {
      logger.error("Unable to parse json file:" + file + " Exception follows.",
          e);
    }

  }
}
