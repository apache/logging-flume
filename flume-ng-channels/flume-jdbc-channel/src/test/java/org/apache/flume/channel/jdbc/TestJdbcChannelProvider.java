package org.apache.flume.channel.jdbc;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import junit.framework.Assert;

import org.apache.flume.Transaction;
import org.apache.flume.channel.jdbc.impl.JdbcChannelProviderImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestJdbcChannelProvider {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(TestJdbcChannelProvider.class);

  private Properties derbyProps = new Properties();
  private File derbyDbDir;
  private JdbcChannelProviderImpl provider;

  @Before
  public void setUp() throws IOException {
    derbyProps.clear();
    derbyProps.put(ConfigurationConstants.CONFIG_CREATE_SCHEMA, "true");
    derbyProps.put(ConfigurationConstants.CONFIG_DATABASE_TYPE, "DERBY");
    derbyProps.put(ConfigurationConstants.CONFIG_JDBC_DRIVER_CLASS,
        "org.apache.derby.jdbc.EmbeddedDriver");

    derbyProps.put(ConfigurationConstants.CONFIG_PASSWORD, "");
    derbyProps.put(ConfigurationConstants.CONFIG_USERNAME, "sa");

    File tmpDir = new File("target/test");
    tmpDir.mkdirs();

    // Use a temp file to create a temporary directory
    File tempFile = File.createTempFile("temp", "_db", tmpDir);
    String absFileName = tempFile.getCanonicalPath();
    tempFile.delete();

    derbyDbDir = new File(absFileName + "_dir");

    if (!derbyDbDir.exists()) {
      derbyDbDir.mkdirs();
    }

    derbyProps.put(ConfigurationConstants.CONFIG_URL,
        "jdbc:derby:" + derbyDbDir.getCanonicalPath() + "/db;create=true");

    LOGGER.info("Derby Properties: " + derbyProps);
  }

  @Test
  public void testDerbySetup() {
    provider = new JdbcChannelProviderImpl();

    provider.initialize(derbyProps);

    Transaction tx1 = provider.getTransaction();
    tx1.begin();

    Transaction tx2 = provider.getTransaction();

    Assert.assertSame(tx1, tx2);
    tx2.begin();
    tx2.close();
    tx1.close();

    Transaction tx3 = provider.getTransaction();
    Assert.assertNotSame(tx1, tx3);

    tx3.begin();
    tx3.close();

    provider.close();
    provider = null;
  }

  @After
  public void tearDown() throws IOException {
    if (provider != null) {
      try {
        provider.close();
      } catch (Exception ex) {
        LOGGER.error("Unable to close provider", ex);
      }
    }
    provider = null;
  }
}
