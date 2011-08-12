package org.apache.flume.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestReporter {

  private Reporter reporter;

  @Before
  public void setUp() {
    reporter = new Reporter();
  }

  @Test
  public void testProgress() {
    Assert.assertEquals("Timestamp starts at zero", 0, reporter.getTimeStamp());
    Assert.assertTrue("Timestamp > 0 after progress", reporter.progress() > 0);
  }

}
