package org.apache.flume.node.nodemanager;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Context;
import org.apache.flume.EventSink;
import org.apache.flume.EventSource;
import org.apache.flume.LogicalNode;
import org.apache.flume.SinkFactory;
import org.apache.flume.SourceFactory;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.node.NodeConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultLogicalNodeManager extends AbstractLogicalNodeManager
    implements NodeConfigurationAware {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultLogicalNodeManager.class);

  private SourceFactory sourceFactory;
  private SinkFactory sinkFactory;

  private ScheduledExecutorService monitorService;
  private LifecycleState lifecycleState;

  public DefaultLogicalNodeManager() {
    lifecycleState = LifecycleState.IDLE;
  }

  @Override
  public void onNodeConfigurationChanged(NodeConfiguration nodeConfiguration) {
    logger.info("Node configuration change:{}", nodeConfiguration);

    /*
     * FIXME: Decide if nodeConfiguration is worth applying. We can't trust the
     * caller to know our config.
     */

    EventSource source = null;
    EventSink sink = null;

    try {
      source = sourceFactory.create(nodeConfiguration.getSourceDefinition());
    } catch (InstantiationException e) {
      logger
          .error(
              "Failed to apply configuration:{} because of source failure:{} - retaining old configuration",
              nodeConfiguration, e.getMessage());
      return;
    }

    try {
      sink = sinkFactory.create(nodeConfiguration.getSinkDefinition());
    } catch (InstantiationException e) {
      logger
          .error(
              "Failed to apply configuration:{} because of sink failure:{} - retaining old configuration",
              nodeConfiguration, e.getMessage());
      return;
    }

    LogicalNode newLogicalNode = new LogicalNode();

    newLogicalNode.setName(nodeConfiguration.getName());
    newLogicalNode.setSource(source);
    newLogicalNode.setSink(sink);

    add(newLogicalNode);
  }

  @Override
  public void start(Context context) throws LifecycleException,
      InterruptedException {

    logger.info("Node manager starting");

    NodeStatusMonitor statusMonitor = new NodeStatusMonitor();

    statusMonitor.setNodeManager(this);

    monitorService = Executors.newScheduledThreadPool(1);
    monitorService.scheduleAtFixedRate(statusMonitor, 0, 3, TimeUnit.SECONDS);

    for (LogicalNode node : getNodes()) {
      try {
        node.start(context);
      } catch (LifecycleException e) {
        logger.error("Failed to start logical node:{}", node);
      } catch (InterruptedException e) {
        logger.error("Interrupted while starting logical node:{}", node);
        lifecycleState = LifecycleState.ERROR;
        throw e;
      }
    }

    logger.debug("Node manager started");

    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop(Context context) throws LifecycleException,
      InterruptedException {

    logger.info("Node manager stopping");

    for (LogicalNode node : getNodes()) {
      try {
        node.stop(context);
      } catch (LifecycleException e) {
        logger.error("Failed to stop logical node:{}", node);
      } catch (InterruptedException e) {
        logger
            .error(
                "Interrupted while stopping logical node:{} - Continuing shutdown anyway!",
                node);
      }
    }

    monitorService.shutdown();

    while (!monitorService.isTerminated()) {
      logger.debug("Waiting for node status monitor to shutdown");
      monitorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    logger.debug("Node manager stopped");

    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

}
