package org.apache.flume.node;

import org.junit.Test;

public class TestFlumeNodeApplication {

  @Test(timeout = 5000)
  public void testApplication() {
    String[] args = new String[] {};

    Application.main(args);
  }

}
