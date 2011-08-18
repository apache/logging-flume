package org.apache.flume.node;

import org.junit.Ignore;
import org.junit.Test;

@Ignore("Causes blocking with no method for clean shutdown")
public class TestFlumeNodeApplication {

  @Test
  public void testApplication() {
    String[] args = new String[] {};

    Application.main(args);
  }

}
