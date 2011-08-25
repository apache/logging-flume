package org.apache.flume.node;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flume.Context;
import org.apache.flume.EventSink;
import org.apache.flume.EventSource;
import org.apache.flume.LogicalNode;
import org.apache.flume.SinkFactory;
import org.apache.flume.SourceFactory;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.node.nodemanager.DefaultLogicalNodeManager;
import org.apache.flume.sink.DefaultSinkFactory;
import org.apache.flume.sink.LoggerSink;
import org.apache.flume.sink.NullSink;
import org.apache.flume.sink.RollingFileSink;
import org.apache.flume.source.DefaultSourceFactory;
import org.apache.flume.source.NetcatSource;
import org.apache.flume.source.SequenceGeneratorSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

  private static final Logger logger = LoggerFactory
      .getLogger(Application.class);

  private String[] args;
  private Set<NodeConfiguration> nodeConfigs;
  private Map<String, Context> contexts;

  private SourceFactory sourceFactory;
  private SinkFactory sinkFactory;

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
    nodeConfigs = new HashSet<NodeConfiguration>();
    sourceFactory = new DefaultSourceFactory();
    sinkFactory = new DefaultSinkFactory();
    contexts = new HashMap<String, Context>();
  }

  public void loadPlugins() {
    sourceFactory.register("seq", SequenceGeneratorSource.class);
    sourceFactory.register("netcat", NetcatSource.class);

    sinkFactory.register("null", NullSink.class);
    sinkFactory.register("logger", LoggerSink.class);
    sinkFactory.register("file-roll", RollingFileSink.class);
  }

  public void parseOptions() throws ParseException {
    Options options = new Options();

    Option option = new Option("n", "node", true, "creates a logical node");
    option.setValueSeparator(',');
    options.addOption(option);

    CommandLineParser parser = new GnuParser();

    CommandLine commandLine = parser.parse(options, args);

    if (commandLine.hasOption("node")) {
      String[] values = commandLine.getOptionValues("node");

      for (String value : values) {
        String[] parts = value.split(":");

        if (parts.length < 3) {
          throw new ParseException(
              "Node definition must be in the format <name>:<source>:<sink>:<context params>");
        }

        DefaultNodeConfiguration nodeConfiguration = new DefaultNodeConfiguration();

        nodeConfiguration.setName(parts[0]);
        nodeConfiguration.setSourceDefinition(parts[1]);
        nodeConfiguration.setSinkDefinition(parts[2]);

        Context context = new Context();

        if (parts.length >= 4) {
          logger.debug("Parsing context parameters:{}", parts[3]);

          String[] contextParts = parts[3].split("\\|");

          for (String contextPart : contextParts) {
            logger.debug("parsing kv pair:{}", contextPart);

            String[] strings = contextPart.split("=");
            context.put(strings[0], strings[1]);
          }

        }

        logger.debug("Created nodeConfig:{} context:{}", nodeConfiguration,
            context);

        contexts.put(nodeConfiguration.getName(), context);

        nodeConfigs.add(nodeConfiguration);
      }
    }
  }

  public void run() throws LifecycleException, InterruptedException,
      InstantiationException {

    final Context context = new Context();
    final FlumeNode node = new FlumeNode();
    NodeManager nodeManager = new DefaultLogicalNodeManager();

    node.setName("node");
    node.setNodeManager(nodeManager);

    Runtime.getRuntime().addShutdownHook(new Thread("node-shutdownHook") {

      @Override
      public void run() {
        try {
          node.stop(context);
        } catch (LifecycleException e) {
          logger.error("Unable to stop node. Exception follows.", e);
        } catch (InterruptedException e) {
          logger
              .error("Interrupted while stopping node. Exception follows.", e);
        }
      }

    });

    node.start(context);
    LifecycleController.waitForOneOf(node, LifecycleState.START_OR_ERROR);

    if (node.getLifecycleState().equals(LifecycleState.START)) {
      for (NodeConfiguration nodeConf : nodeConfigs) {
        EventSource source = sourceFactory.create(nodeConf
            .getSourceDefinition());
        EventSink sink = sinkFactory.create(nodeConf.getSinkDefinition());

        Configurables.configure(source, contexts.get(nodeConf.getName()));
        Configurables.configure(sink, contexts.get(nodeConf.getName()));

        LogicalNode logicalNode = new LogicalNode();

        logicalNode.setName(nodeConf.getName());
        logicalNode.setSource(source);
        logicalNode.setSink(sink);

        nodeManager.add(logicalNode);
      }
    }

    LifecycleController.waitForOneOf(node, LifecycleState.STOP_OR_ERROR);
  }

  public String[] getArgs() {
    return args;
  }

  public void setArgs(String[] args) {
    this.args = args;
  }

}
