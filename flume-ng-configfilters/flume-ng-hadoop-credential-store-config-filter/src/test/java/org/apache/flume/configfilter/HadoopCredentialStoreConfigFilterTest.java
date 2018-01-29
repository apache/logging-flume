/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.flume.configfilter;

import org.apache.flume.conf.FlumeConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.alias.CredentialShell;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flume.configfilter.HadoopCredentialStoreConfigFilter
    .CREDENTIAL_PROVIDER_PATH;
import static org.apache.flume.configfilter.HadoopCredentialStoreConfigFilter
    .CREDSTORE_JAVA_KEYSTORE_PROVIDER_PASSWORD_FILE_CONFIG_KEY;
import static org.apache.flume.configfilter.HadoopCredentialStoreConfigFilter.HADOOP_SECURITY;
import static org.junit.Assert.assertEquals;

public class HadoopCredentialStoreConfigFilterTest {

  public static final String AGENT = "agent";
  public static final String CONFIGFILTERS = AGENT + ".configfilters";
  public static final String SINKS = AGENT + ".sinks";
  public static final String SOURCES = AGENT + ".sources";
  public static final String CHANNELS = AGENT + ".channels";
  private static final Map<String, String> PROPERTIES =  new HashMap();
  private static String providerPathDefault;
  private static String providerPathEnv;
  private static String providerPathPwdFile;

  @ClassRule
  public static final EnvironmentVariables environmentVariables
      = new EnvironmentVariables();
  private static File fileDefault;
  private static File fileEnvPassword;
  private static File fileFilePassword;
  private static File keystorePasswordFile;

  @BeforeClass
  public static void setUpClass() throws Exception {
    // Set the default properties
    PROPERTIES.put(SOURCES, "s1");
    PROPERTIES.put(SOURCES + ".s1.type", "s1_type");
    PROPERTIES.put(SOURCES + ".s1.channels", "c1");
    PROPERTIES.put(SOURCES + ".s1.password", "${f1['my_password_key1']}");
    PROPERTIES.put(CHANNELS, "c1");
    PROPERTIES.put(CHANNELS + ".c1.type", "c1_type");
    PROPERTIES.put(CHANNELS + ".c1.secret", "${f1['my_password_key2']}");
    PROPERTIES.put(SINKS, "k1");
    PROPERTIES.put(SINKS + ".k1.type", "k1_type");
    PROPERTIES.put(SINKS + ".k1.channel", "c1");
    PROPERTIES.put(SINKS + ".k1.token", "${f1['my_password_key3']}");
    PROPERTIES.put(CONFIGFILTERS, "f1");
    PROPERTIES.put(CONFIGFILTERS + ".f1.type", "hadoop");

    // get a temp directory for
    fileDefault = Files.createTempFile("test-default-pwd-", ".jceks").toFile();
    fileDefault.delete();
    fileEnvPassword = Files.createTempFile("test-env-pwd-", ".jceks").toFile();
    fileEnvPassword.delete();
    fileFilePassword = Files.createTempFile("test-file-pwd-", ".jceks").toFile();
    fileFilePassword.delete();

    providerPathDefault = "jceks://file/" + fileDefault.getAbsolutePath();
    providerPathEnv = "jceks://file/" + fileEnvPassword.getAbsolutePath();
    providerPathPwdFile = "jceks://file/" + fileFilePassword.getAbsolutePath();

    runCommand("create my_password_key1 -value filtered1_default -provider "
        + providerPathDefault, new Configuration());
    runCommand("create my_password_key2 -value filtered2_default -provider "
        + providerPathDefault, new Configuration());
    runCommand("create my_password_key3 -value filtered3_default -provider "
        + providerPathDefault, new Configuration());

    Configuration conf = new Configuration();
    conf.set(
        HADOOP_SECURITY + CREDSTORE_JAVA_KEYSTORE_PROVIDER_PASSWORD_FILE_CONFIG_KEY,
        "test-password.txt"
    );
    runCommand("create my_password_key1 -value filtered1_file -provider "
        + providerPathPwdFile, conf);
    runCommand("create my_password_key2 -value filtered2_file -provider "
        + providerPathPwdFile, conf);
    runCommand("create my_password_key3 -value filtered3_file -provider "
        + providerPathPwdFile, conf);

    environmentVariables.set("HADOOP_CREDSTORE_PASSWORD", "envSecret");

    runCommand("create my_password_key1 -value filtered1_env -provider "
        + providerPathEnv, new Configuration());
    runCommand("create my_password_key2 -value filtered2_env -provider "
        + providerPathEnv, new Configuration());
    runCommand("create my_password_key3 -value filtered3_env -provider "
        + providerPathEnv, new Configuration());

  }

  private static void runCommand(String c, Configuration conf) throws Exception {
    ToolRunner.run(conf, new CredentialShell(), c.split(" "));
  }

  @AfterClass
  public static void tearDown() {
    fileDefault.delete();
    fileEnvPassword.delete();
    fileFilePassword.delete();
  }

  @Before
  public void setUp() {
    String[] objects = System.getenv().keySet().toArray(new String[0]);
    environmentVariables.clear(objects);
  }

  @Test
  public void filterDefaultPasswordFile() {
    Map<String, String> properties = new HashMap<>(PROPERTIES);
    properties.put(CONFIGFILTERS + ".f1." + CREDENTIAL_PROVIDER_PATH, providerPathDefault);
    FlumeConfiguration flumeConfiguration = new FlumeConfiguration(properties);
    String actual;

    actual = flumeConfiguration.getConfigurationFor(AGENT).getSourceContext().get("s1")
        .getString("password");
    assertEquals("filtered1_default", actual);
    actual = flumeConfiguration.getConfigurationFor(AGENT).getChannelContext().get("c1")
        .getString("secret");
    assertEquals("filtered2_default", actual);
    actual = flumeConfiguration.getConfigurationFor(AGENT).getSinkContext().get("k1")
        .getString("token");
    assertEquals("filtered3_default", actual);

  }

  @Test
  public void filterWithEnvPassword() {
    Map<String, String> properties = new HashMap<>(PROPERTIES);
    properties.put(CONFIGFILTERS + ".f1." + CREDENTIAL_PROVIDER_PATH, providerPathEnv);
    environmentVariables.set("HADOOP_CREDSTORE_PASSWORD","envSecret");
    FlumeConfiguration flumeConfiguration = new FlumeConfiguration(properties);
    String actual;

    actual = flumeConfiguration.getConfigurationFor(AGENT).getSourceContext().get("s1")
        .getString("password");
    assertEquals("filtered1_env", actual);
    actual = flumeConfiguration.getConfigurationFor(AGENT).getChannelContext().get("c1")
        .getString("secret");
    assertEquals("filtered2_env", actual);
    actual = flumeConfiguration.getConfigurationFor(AGENT).getSinkContext().get("k1")
        .getString("token");
    assertEquals("filtered3_env", actual);

  }

  @Test
  public void filterWithEnvNoPassword() {
    Map<String, String> properties = new HashMap<>(PROPERTIES);
    properties.put(CONFIGFILTERS + ".f1." + CREDENTIAL_PROVIDER_PATH, providerPathEnv);
    FlumeConfiguration flumeConfiguration = new FlumeConfiguration(properties);
    assertEquals(
        "${f1['my_password_key1']}",
        flumeConfiguration.getConfigurationFor(AGENT).getSourceContext().get("s1")
            .getString("password")
    );
  }



  @Test
  public void filterWithPasswordFile() throws IOException {
    Map<String, String> properties = new HashMap<>(PROPERTIES);
    properties.put(CONFIGFILTERS + ".f1." + CREDENTIAL_PROVIDER_PATH, providerPathPwdFile);
    properties.put(
        CONFIGFILTERS + ".f1." + CREDSTORE_JAVA_KEYSTORE_PROVIDER_PASSWORD_FILE_CONFIG_KEY,
        "test-password.txt"
    );
    FlumeConfiguration flumeConfiguration = new FlumeConfiguration(properties);
    String actual;

    actual = flumeConfiguration.getConfigurationFor(AGENT).getSourceContext().get("s1")
        .getString("password");
    assertEquals("filtered1_file", actual);
    actual = flumeConfiguration.getConfigurationFor(AGENT).getChannelContext().get("c1")
        .getString("secret");
    assertEquals("filtered2_file", actual);
    actual = flumeConfiguration.getConfigurationFor(AGENT).getSinkContext().get("k1")
        .getString("token");
    assertEquals("filtered3_file", actual);

  }

  @Test
  public void filterWithPasswordFileWrongPassword() throws IOException {
    Map<String, String> properties = new HashMap<>(PROPERTIES);
    properties.put(CONFIGFILTERS + ".f1." + CREDENTIAL_PROVIDER_PATH, providerPathPwdFile);
    properties.put(
        CONFIGFILTERS + ".f1." + CREDSTORE_JAVA_KEYSTORE_PROVIDER_PASSWORD_FILE_CONFIG_KEY,
        "test-password.txt2"
    );
    FlumeConfiguration flumeConfiguration = new FlumeConfiguration(properties);
    assertEquals(
        "${f1['my_password_key1']}",
        flumeConfiguration.getConfigurationFor(AGENT).getSourceContext().get("s1")
            .getString("password")
    );
  }

  @Test
  public void filterWithPasswordFileNoPasswordFile() throws IOException {
    Map<String, String> properties = new HashMap<>(PROPERTIES);
    properties.put(CONFIGFILTERS + ".f1." + CREDENTIAL_PROVIDER_PATH, providerPathPwdFile);
    FlumeConfiguration flumeConfiguration = new FlumeConfiguration(properties);
    assertEquals(
        "${f1['my_password_key1']}",
        flumeConfiguration.getConfigurationFor(AGENT).getSourceContext().get("s1")
            .getString("password")
    );
  }
}