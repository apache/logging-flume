package org.apache.flume.node;

import org.apache.flume.Context;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleException;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.node.nodemanager.DefaultLogicalNodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

  private static final Logger logger = LoggerFactory
      .getLogger(Application.class);

  private String[] args;

  public static void main(String[] args) {
    Application application = new Application();

    application.setArgs(args);

    try {
      application.run();
    } catch (Exception e) {
      logger.error("A fatal error occurred while running. Exception follows.",
          e);
    }
  }

  public void run() throws LifecycleException, InterruptedException {
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
          // TODO Auto-generated catch block
          e.printStackTrace();
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }

    });

    node.start(context);

    LifecycleController.waitForOneOf(node, LifecycleState.STOP_OR_ERROR);
  }

  public String[] getArgs() {
    return args;
  }

  public void setArgs(String[] args) {
    this.args = args;
  }

}
