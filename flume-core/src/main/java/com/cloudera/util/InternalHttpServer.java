package com.cloudera.util;

import java.io.File;

import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * <p>
 * An embedded Jetty HTTP server that support both normal and exploded war file
 * deployment. Those that wish to expose HTTP services should create an instance
 * of this class, configure the server via accessor methods, and then call
 * {@link #start()}.
 * </p>
 * <p>
 * Resources internally are allocated upon the first call to {@link #start()}.
 * This includes scanning of the configured webapp directory for applications if
 * {@link #getScanForApps()} is true (the default). Mostly this class is a thin
 * wrapper around Jetty's {@link Server} class and behaves as Jetty does. Both
 * traditional and exploded war formats are supported in the webapp directory.
 * In the case of exploded directories, the directory name is used as the
 * context. For war files, everything from the first instance of ".war" to the
 * end of the file name (inclusive) is stripped and the remainder is used for
 * the context name.
 * </p>
 * <p>
 * Name examples:
 * </p>
 * <table>
 * <tr>
 * <td>Name</td>
 * <td>Type</td>
 * <td>Context</td>
 * </tr>
 * <tr>
 * <td>app.war</td>
 * <td>file</td>
 * <td>app</td>
 * </tr>
 * <tr>
 * <td>app</td>
 * <td>dir</td>
 * <td>app</td>
 * </tr>
 * <tr>
 * <td>app.war</td>
 * <td>dir</td>
 * <td>app.war</td>
 * </tr>
 * <tr>
 * <td>app.war.war</td>
 * <td>file</td>
 * <td>app</td>
 * </tr>
 * <tr>
 * <td>
 * </table>
 * <p>
 * Example usage:
 * </p>
 * 
 * <pre>
 * InternalHttpServer server = new InternalHttpServer();
 * 
 * // applicationHome/webapps/ will be scanned for war files and directories.
 * server.setWebappDir(new File(applicationHome, &quot;webapps&quot;));
 * server.setPort(8080);
 * server.setBindAddress(&quot;0.0.0.0&quot;);
 * 
 * server.start();
 * 
 * // at some later time...
 * server.stop();
 * </pre>
 */
public class InternalHttpServer {

  private static final Logger logger = LoggerFactory
      .getLogger(InternalHttpServer.class);

  private Server server;
  private File webappDir;
  private int port;
  private String bindAddress;
  private boolean scanForApps;

  public InternalHttpServer() {
    port = 0;
    bindAddress = "0.0.0.0";
    scanForApps = true;
  }

  public void initialize() {
    if (server == null) {

      server = new Server();
      Connector connector = new SelectChannelConnector();

      connector.setPort(port);
      connector.setHost(bindAddress);

      server.addConnector(connector);
    }
  }

  protected void registerApplications() {
    logger.debug("Registering webapps in {}", webappDir);

    if (webappDir.isDirectory()) {
      for (File entry : webappDir.listFiles()) {
        tryRegisterApplication(server, entry);
      }
    } else {
      tryRegisterApplication(server, webappDir);
    }
  }

  private boolean tryRegisterApplication(Server server, File path) {
    String name;

    logger.debug("checking {}", path);

    if (path.isFile()) {
      int idx = path.getName().indexOf(".war");

      if (idx > -1) {
        name = path.getName().substring(0, idx);
      } else {
        return false;
      }
    } else {
      name = path.getName();
    }

    logger.debug("creating context {} -> {}", name, path);

    WebAppContext handler = new WebAppContext(path.getPath(), "/" + name);

    handler.setParentLoaderPriority(true);

    server.addHandler(handler);

    return true;
  }

  /**
   * <p>
   * Start a configured HTTP server. Users should have already injected all the
   * necessary configuration parameters at this point. It is not considered safe
   * to call start() more than once and will lead to undefined behavior.
   * </p>
   * <p>
   * The configured webappDir is not scanned for applications until start() is
   * called.
   * </p>
   * 
   * @throws InternalHttpServerException
   */
  public void start() {
    Preconditions.checkState(webappDir != null, "Webapp dir can not be null");

    initialize();

    if (scanForApps) {
      registerApplications();
    } else {
      logger.info("Not scanning for webapps");
    }

    logger.info("Starting internal HTTP server");

    try {
      server.start();

      logger.info("Server started");
    } catch (Exception e) {
      logger.warn("Caught exception during HTTP server start.", e);

      throw new InternalHttpServerException("Unable to start HTTP server", e);
    }
  }

  /**
   * Stop the running HTTP server. If {@link #start()} has never been called,
   * this is a no-op. This call blocks until the internal Jetty instance is
   * stopped (to the extent the Jetty {@link Server#stop()} call blocks).
   * 
   * @throws InternalHttpServerException
   */
  public void stop() {
    if (server == null) {
      return;
    }

    logger.info("Stopping internal HTTP server");

    try {
      server.stop();
    } catch (Exception e) {
      logger.warn("Caught exception during HTTP server stop.", e);

      throw new InternalHttpServerException("Unable to stop HTTP server", e);
    }
  }

  @Override
  public String toString() {
    return "{ bindAddress:" + bindAddress + " webappDir:" + webappDir
        + " port:" + port + " scanForApps:" + scanForApps + " server:" + server
        + " }";
  }

  public Server getServer() {
    return server;
  }

  public void setServer(Server server) {
    this.server = server;
  }

  public File getWebappDir() {
    return webappDir;
  }

  public void setWebappDir(File webappDir) {
    this.webappDir = webappDir;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getBindAddress() {
    return bindAddress;
  }

  public void setBindAddress(String bindAddress) {
    this.bindAddress = bindAddress;
  }

  public boolean getScanForApps() {
    return scanForApps;
  }

  public void setScanForApps(boolean scanForApps) {
    this.scanForApps = scanForApps;
  }

  public static class InternalHttpServerException extends RuntimeException {

    private static final long serialVersionUID = -4936285404574873547L;

    public InternalHttpServerException() {
      super();
    }

    public InternalHttpServerException(String msg) {
      super(msg);
    }

    public InternalHttpServerException(String msg, Throwable t) {
      super(msg, t);
    }

    public InternalHttpServerException(Throwable t) {
      super(t);
    }

  }

}
