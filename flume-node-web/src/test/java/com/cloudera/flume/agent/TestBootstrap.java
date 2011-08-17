package com.cloudera.flume.agent;

import java.io.File;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.util.InternalHttpServer;

public class TestBootstrap {

  private static final Logger logger = LoggerFactory
      .getLogger(TestBootstrap.class);

  private InternalHttpServer httpServer;

  @Before
  public void setUp() {
    httpServer = new InternalHttpServer();
  }

  @Test
  public void testBootstrap() throws InterruptedException {
    Assert.assertNotNull(httpServer);

    logger.debug("httpServer:{}", httpServer);

    httpServer.setPort(0);
    httpServer.setWebappDir(new File("src/main"));

    httpServer.start();

    Thread.sleep(3000);

    httpServer.stop();
  }

}
