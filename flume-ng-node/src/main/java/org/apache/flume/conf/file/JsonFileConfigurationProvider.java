package org.apache.flume.conf.file;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Channel;
import org.apache.flume.ChannelFactory;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.apache.flume.SinkRunner;
import org.apache.flume.Source;
import org.apache.flume.SourceFactory;
import org.apache.flume.SourceRunner;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.node.ConfigurationProvider;
import org.apache.flume.node.nodemanager.NodeConfigurationAware;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class JsonFileConfigurationProvider implements ConfigurationProvider {

  private static final Logger logger = LoggerFactory
      .getLogger(JsonFileConfigurationProvider.class);

  private File file;
  private ChannelFactory channelFactory;
  private SourceFactory sourceFactory;
  private SinkFactory sinkFactory;
  private NodeConfigurationAware configurationAware;

  private LifecycleState lifecycleState;
  private ScheduledExecutorService executorService;
  private CounterGroup counterGroup;

  public JsonFileConfigurationProvider() {
    lifecycleState = LifecycleState.IDLE;
    counterGroup = new CounterGroup();
  }

  @Override
  public String toString() {
    return "{ file:" + file + " counterGroup:" + counterGroup + " }";
  }

  @Override
  public void start() {
    logger.info("JSON configuration provider starting");

    Preconditions.checkState(file != null,
        "The parameter file must not be null");

    executorService = Executors
        .newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("conf-file-poller-%d")
                .build());

    FileWatcherRunnable fileWatcherRunnable = new FileWatcherRunnable();

    fileWatcherRunnable.file = file;
    fileWatcherRunnable.counterGroup = counterGroup;

    executorService.scheduleAtFixedRate(fileWatcherRunnable, 0, 30,
        TimeUnit.SECONDS);

    lifecycleState = LifecycleState.START;

    logger.debug("JSON configuration provider started");
  }

  @Override
  public void stop() {
    logger.info("JSON configuration provider stopping");

    executorService.shutdown();

    while (!executorService.isTerminated()) {
      try {
        logger.debug("Waiting for file watcher to terminate");
        executorService.awaitTermination(500, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger.debug("Interrupted while waiting for file watcher to terminate");
        Thread.currentThread().interrupt();
      }
    }

    lifecycleState = LifecycleState.STOP;

    logger.debug("JSON configuration provider stopped");
  }

  private void loadSources(SimpleNodeConfiguration conf,
      List<Map<String, Object>> defs) throws InstantiationException {

    logger.debug("Loading sources");

    for (Map<String, Object> sourceDef : defs) {
      logger.debug("source:{}", sourceDef);

      if (sourceDef.containsKey("type")) {
        Source source = sourceFactory.create((String) sourceDef.get("type"));
        Channel channel = conf.getChannels().get(sourceDef.get("channel"));

        Context context = new Context();
        context.setParameters(sourceDef);

        Configurables.configure(source, context);

        if (channel != null) {
          source.setChannel(channel);
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
        Sink sink = sinkFactory.create((String) sinkDef.get("type"));
        Channel channel = conf.getChannels().get(sinkDef.get("channel"));

        Context context = new Context();
        context.setParameters(sinkDef);

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
        Channel channel = channelFactory
            .create((String) channelDef.get("type"));

        Context context = new Context();
        context.setParameters(channelDef);

        conf.getChannels().put((String) channelDef.get("name"), channel);
      }
    }
  }

  private synchronized void load() {
    SimpleNodeConfiguration flumeConf = new SimpleNodeConfiguration();
    ObjectMapper mapper = new ObjectMapper();

    try {
      @SuppressWarnings("unchecked")
      Map<String, Map<String, List<Map<String, Object>>>> jsonTree = mapper
          .readValue(file, Map.class);

      for (Entry<String, Map<String, List<Map<String, Object>>>> hostDef : jsonTree
          .entrySet()) {

        logger.debug("host:{}", hostDef);

        /*
         * NB: Because load{Sources,Sinks} wire up dependencies (i.e. channels),
         * loadChannels must always be executed first.
         */
        loadChannels(flumeConf, hostDef.getValue().get("channels"));
        loadSources(flumeConf, hostDef.getValue().get("sources"));
        loadSinks(flumeConf, hostDef.getValue().get("sinks"));
      }

      logger.debug("Loaded conf:{}", flumeConf);

      configurationAware.onNodeConfigurationChanged(flumeConf);
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

  public File getFile() {
    return file;
  }

  public void setFile(File file) {
    this.file = file;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  public ChannelFactory getChannelFactory() {
    return channelFactory;
  }

  public void setChannelFactory(ChannelFactory channelFactory) {
    this.channelFactory = channelFactory;
  }

  public SourceFactory getSourceFactory() {
    return sourceFactory;
  }

  public void setSourceFactory(SourceFactory sourceFactory) {
    this.sourceFactory = sourceFactory;
  }

  public SinkFactory getSinkFactory() {
    return sinkFactory;
  }

  public void setSinkFactory(SinkFactory sinkFactory) {
    this.sinkFactory = sinkFactory;
  }

  public NodeConfigurationAware getConfigurationAware() {
    return configurationAware;
  }

  public void setConfigurationAware(NodeConfigurationAware configurationAware) {
    this.configurationAware = configurationAware;
  }

  public class FileWatcherRunnable implements Runnable {

    private File file;
    private CounterGroup counterGroup;

    private long lastChange;

    @Override
    public void run() {
      logger.debug("Checking file:{} for changes", file);

      counterGroup.incrementAndGet("file.checks");

      long lastModified = file.lastModified();

      if (lastModified > lastChange) {
        logger.info("Reloading configuration file:{}", file);

        counterGroup.incrementAndGet("file.loads");

        lastChange = lastModified;

        try {
          load();
        } catch (Exception e) {
          logger.error("Failed to load configuration data. Exception follows.",
              e);
        }
      }
    }
  }

}
