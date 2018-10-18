package org.apache.flume.util;

import java.util.Arrays;
import java.util.Collection;

import org.junit.runners.Parameterized.Parameters;

public abstract class AbstractSSLUtilListTest extends AbstractSSLUtilTest {
  @Parameters
  public static Collection<?> data() {
    return Arrays.asList(new Object[][]{
      // system property value, environment variable value, expected value
      { null, null, null },
      { "sysprop", null, "sysprop" },
      { "sysprop,sysprop", null, "sysprop sysprop" },
      { null, "envvar", "envvar" },
      { null, "envvar,envvar", "envvar envvar" },
      { "sysprop", "envvar", "sysprop" },
      { "sysprop,sysprop", "envvar,envvar", "sysprop sysprop" }
    });
  }

  protected AbstractSSLUtilListTest(String sysPropValue, String envVarValue, String expectedValue) {
    super(sysPropValue, envVarValue, expectedValue);
  }
}
