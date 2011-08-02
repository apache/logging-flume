package com.cloudera.util;

import java.io.File;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalHttpServerTest {

  private static final Logger logger = LoggerFactory
      .getLogger(InternalHttpServerTest.class);

  private InternalHttpServer httpServer;

  @Before
  public void setUp() {
    httpServer = new InternalHttpServer();
  }

  @Test
  public void testStartInvalidState() {
    boolean success = false;

    try {
      httpServer.start();
      success = true;
    } catch (IllegalStateException e) {
      logger.info("Caught expected exception: {}", e.getMessage());
    }

    Assert.assertFalse(success);
  }

  @Test
  public void testStart() {
    boolean success = false;

    httpServer.setWebappDir(new File(getClass().getClassLoader()
        .getResource("test-webroot").getFile()));

    try {
      httpServer.start();
      success = true;
    } catch (IllegalStateException e) {
      logger.error("Caught exception:", e);
    }

    Assert.assertTrue(success);

    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    success = false;

    try {
      httpServer.stop();
      success = true;
    } catch (IllegalStateException e) {
      logger.error("Caught exception:", e);
    }

    Assert.assertTrue(success);
  }

}
