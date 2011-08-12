package org.apache.flume;

import org.apache.flume.Reporter;
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
    Assert.assertTrue("Timestamp > 0 after progress", reporter.progress() > 0);
  }

}
