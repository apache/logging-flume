package org.apache.flume.node;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flume.ChannelFactory;
import org.apache.flume.SinkFactory;
import org.apache.flume.SourceFactory;
import org.apache.flume.channel.DefaultChannelFactory;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.file.JsonFileConfigurationProvider;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.node.nodemanager.DefaultLogicalNodeManager;
import org.apache.flume.sink.DefaultSinkFactory;
import org.apache.flume.sink.LoggerSink;
import org.apache.flume.sink.NullSink;
import org.apache.flume.sink.RollingFileSink;
import org.apache.flume.sink.hdfs.HDFSEventSink;
import org.apache.flume.source.DefaultSourceFactory;
import org.apache.flume.source.NetcatSource;
import org.apache.flume.source.SequenceGeneratorSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class Application {

  private static final Logger logger = LoggerFactory
      .getLogger(Application.class);

  private String[] args;
  private File configurationFile;

  private SourceFactory sourceFactory;
  private SinkFactory sinkFactory;
  private ChannelFactory channelFactory;

  public static void main(String[] args) {
    Application application = new Application();

    application.setArgs(args);

    try {
      application.loadPlugins();
      application.parseOptions();
      application.run();
    } catch (ParseException e) {
      logger.error(e.getMessage());
    } catch (Exception e) {
      logger.error("A fatal error occurred while running. Exception follows.",
          e);
    }
  }

  public Application() {
    sourceFactory = new DefaultSourceFactory();
    sinkFactory = new DefaultSinkFactory();
    channelFactory = new DefaultChannelFactory();
  }

  public void loadPlugins() {
    channelFactory.register("memory", MemoryChannel.class);

    sourceFactory.register("seq", SequenceGeneratorSource.class);
    sourceFactory.register("netcat", NetcatSource.class);

    sinkFactory.register("null", NullSink.class);
    sinkFactory.register("logger", LoggerSink.class);
    sinkFactory.register("file-roll", RollingFileSink.class);
    sinkFactory.register("hdfs", HDFSEventSink.class);
  }

  public void parseOptions() throws ParseException {
    Options options = new Options();

    Option option = new Option("n", "node", true, "creates a logical node");
    option.setValueSeparator(',');
    options.addOption(option);

    option = new Option("f", "conf-file", true, "specify a conf file");
    options.addOption(option);

    CommandLineParser parser = new GnuParser();

    CommandLine commandLine = parser.parse(options, args);

    if (commandLine.hasOption('f')) {
      configurationFile = new File(commandLine.getOptionValue('f'));
    }
  }

  public void run() throws LifecycleException, InterruptedException,
      InstantiationException {

    final FlumeNode node = new FlumeNode();
    DefaultLogicalNodeManager nodeManager = new DefaultLogicalNodeManager();
    JsonFileConfigurationProvider configurationProvider = new JsonFileConfigurationProvider();

    configurationProvider.setChannelFactory(channelFactory);
    configurationProvider.setSourceFactory(sourceFactory);
    configurationProvider.setSinkFactory(sinkFactory);

    Preconditions.checkState(configurationFile != null,
        "Configuration file not specified");

    configurationProvider.setConfigurationAware(nodeManager);
    configurationProvider.setFile(configurationFile);

    node.setName("node");
    node.setNodeManager(nodeManager);
    node.setConfigurationProvider(configurationProvider);

    Runtime.getRuntime().addShutdownHook(new Thread("node-shutdownHook") {

      @Override
      public void run() {
        node.stop();
      }

    });

    node.start();
    LifecycleController.waitForOneOf(node, LifecycleState.START_OR_ERROR);

    /*
     * if (node.getLifecycleState().equals(LifecycleState.START)) { for
     * (NodeConfiguration nodeConf : nodeConfigs) { Source source =
     * sourceFactory.create(nodeConf.getSourceDefinition()); Sink sink =
     * sinkFactory.create(nodeConf.getSinkDefinition()); Channel channel = new
     * MemoryChannel();
     * 
     * Configurables.configure(source, contexts.get(nodeConf.getName()));
     * Configurables.configure(sink, contexts.get(nodeConf.getName()));
     * 
     * source.setChannel(channel); sink.setChannel(channel);
     * 
     * LogicalNode logicalNode = new LogicalNode();
     * 
     * logicalNode.setName(nodeConf.getName());
     * 
     * SourceRunner sourceRunner = SourceRunner.forSource(source); SinkRunner
     * sinkRunner = SinkRunner.forSink(sink);
     * 
     * logicalNode.setSourceRunner(sourceRunner);
     * logicalNode.setSinkRunner(sinkRunner);
     * 
     * nodeManager.add(logicalNode); } }
     */

    LifecycleController.waitForOneOf(node, LifecycleState.STOP_OR_ERROR);
  }

  public String[] getArgs() {
    return args;
  }

  public void setArgs(String[] args) {
    this.args = args;
  }

}
