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
import org.apache.flume.CounterGroup;
import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.apache.flume.SinkRunner;
import org.apache.flume.Source;
import org.apache.flume.SourceFactory;
import org.apache.flume.SourceRunner;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class JsonFileConfigurationProvider implements LifecycleAware {

  private static final Logger logger = LoggerFactory
      .getLogger(JsonFileConfigurationProvider.class);

  private File file;
  private ChannelFactory channelFactory;
  private SourceFactory sourceFactory;
  private SinkFactory sinkFactory;

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
  public synchronized void start() {
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
  public synchronized void stop() {
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

  private void loadSources(JsonFlumeConfiguration conf,
      List<Map<String, Object>> defs) throws InstantiationException {

    logger.debug("Loading sources");

    for (Map<String, Object> source : defs) {
      logger.debug("source:{}", source);

      if (source.containsKey("type")) {
        Source s = sourceFactory.create((String) source.get("type"));

        // s.setChannel(knownChannels.get(source.get("channel")));
        conf.getSourceRunners().add(SourceRunner.forSource(s));
      } else {
        throw new IllegalArgumentException("Illegal source definition:"
            + source + " - Missing type.");
      }
    }
  }

  private void loadSinks(JsonFlumeConfiguration conf,
      List<Map<String, Object>> defs) throws InstantiationException {

    logger.debug("Loading sinks");

    for (Map<String, Object> sink : defs) {
      logger.debug("sink:{}", sink);

      if (sink.containsKey("type")) {
        Sink s = sinkFactory.create((String) sink.get("type"));
        // s.setChannel(knownChannels.get(source.get("channel")));
        conf.getSinkRunners().add(SinkRunner.forSink(s));
      }
    }
  }

  private void loadChannels(JsonFlumeConfiguration conf,
      List<Map<String, Object>> defs) throws InstantiationException {

    logger.debug("Loading channels");

    for (Map<String, Object> channelDef : defs) {
      logger.debug("channel:{}", channelDef);

      if (channelDef.containsKey("type")) {
        Channel channel = channelFactory
            .create((String) channelDef.get("type"));

        conf.getChannels().add(channel);
      }
    }

  }

  private synchronized void load() {
    JsonFlumeConfiguration flumeConf = new JsonFlumeConfiguration();
    ObjectMapper mapper = new ObjectMapper();

    try {
      @SuppressWarnings("unchecked")
      Map<String, Map<String, List<Map<String, Object>>>> jsonTree = mapper
          .readValue(file, Map.class);

      for (Entry<String, Map<String, List<Map<String, Object>>>> hostDef : jsonTree
          .entrySet()) {

        logger.debug("host:{}", hostDef);

        loadSources(flumeConf, hostDef.getValue().get("sources"));
        loadSinks(flumeConf, hostDef.getValue().get("sinks"));
        loadChannels(flumeConf, hostDef.getValue().get("channels"));
      }

      logger.debug("Loaded conf:{}", flumeConf);
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

        load();
      }
    }
  }

}
