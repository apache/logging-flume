package org.apache.flume.node.nodemanager;

import java.util.concurrent.ExecutorService;
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
import org.apache.flume.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class DefaultLogicalNodeManager extends AbstractLogicalNodeManager
    implements NodeConfigurationAware {

  private static final Logger logger = LoggerFactory
      .getLogger(DefaultLogicalNodeManager.class);

  private SourceFactory sourceFactory;
  private SinkFactory sinkFactory;

  private ExecutorService commandProcessorService;
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
  public boolean add(LogicalNode node) {
    Preconditions.checkState(getLifecycleState().equals(LifecycleState.START),
        "You can not add nodes to a manager that hasn't been started");

    if (super.add(node)) {
      startNode(node);

      return true;
    }

    return false;
  }

  @Override
  public boolean remove(LogicalNode node) {
    Preconditions.checkState(getLifecycleState().equals(LifecycleState.START),
        "You can not remove nodes from a manager that hasn't been started");

    if (super.remove(node)) {
      stopNode(node);

      return true;
    }

    return false;
  }

  public void startNode(LogicalNode node) {
    NodeStartCommand task = new NodeStartCommand();

    task.context = new Context();
    task.node = node;
    task.nodeManager = this;

    commandProcessorService.submit(task);
  }

  public void stopNode(LogicalNode node) {
    NodeRemoveCommand task = new NodeRemoveCommand();

    task.context = new Context();
    task.node = node;
    task.nodeManager = this;

    commandProcessorService.submit(task);
  }

  @Override
  public void start(Context context) throws LifecycleException,
      InterruptedException {

    logger.info("Node manager starting");

    NodeStatusMonitor statusMonitor = new NodeStatusMonitor();

    statusMonitor.setNodeManager(this);

    commandProcessorService = Executors
        .newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(
            "nodeManager-commandProcessor-%d").build());
    monitorService = Executors.newScheduledThreadPool(
        1,
        new ThreadFactoryBuilder().setNameFormat(
            "nodeManager-monitorService-%d").build());

    monitorService.scheduleAtFixedRate(statusMonitor, 0, 3, TimeUnit.SECONDS);

    logger.debug("Node manager started");

    lifecycleState = LifecycleState.START;
  }

  @Override
  public void stop(Context context) throws LifecycleException,
      InterruptedException {

    logger.info("Node manager stopping");

    for (LogicalNode node : getNodes()) {
      stopNode(node);
    }

    monitorService.shutdown();

    while (!monitorService.isTerminated()) {
      logger.debug("Waiting for node status monitor to shutdown");
      monitorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    commandProcessorService.shutdown();

    while (!commandProcessorService.isTerminated()) {
      logger.debug("Waiting for command processor to shutdown");
      commandProcessorService.awaitTermination(1, TimeUnit.SECONDS);
    }

    logger.debug("Node manager stopped");

    lifecycleState = LifecycleState.STOP;
  }

  @Override
  public LifecycleState getLifecycleState() {
    return lifecycleState;
  }

  public static class NodeStartCommand implements Runnable {

    public Context context;
    public LogicalNode node;
    public NodeManager nodeManager;

    @Override
    public void run() {
      if (node.getLifecycleState().equals(LifecycleState.START)) {
        logger.warn("Ignoring an attempt to start a running node:{}", node);
        return;
      }

      try {
        node.start(context);
      } catch (LifecycleException e) {
        logger.error("Failed to start node:" + node + " Exception follows.", e);
      } catch (InterruptedException e) {
        logger.info("Interrupted while starting node:" + node
            + " Almost certainly shutting down (or a serious bug)");
      }
    }

  }

  public static class NodeRemoveCommand implements Runnable {

    public Context context;
    public LogicalNode node;
    public NodeManager nodeManager;

    @Override
    public void run() {
      if (!node.getLifecycleState().equals(LifecycleState.START)) {
        logger.warn("Ignoring an attempt to stop a non-running node:{}", node);
        return;
      }

      try {
        node.stop(context);
      } catch (LifecycleException e) {
        logger.error("Failed to stop node:" + node + " Exception follows.", e);
      } catch (InterruptedException e) {
        logger.info("Interrupted while starting node:" + node
            + " Almost certainly shutting down (or a serious bug)");
      }
    }

  }

}
