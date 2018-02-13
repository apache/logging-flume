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

import static org.apache.flume.configfilter.HadoopCredentialStoreConfigFilter.CREDENTIAL_PROVIDER_PATH;
import static org.apache.flume.configfilter.HadoopCredentialStoreConfigFilter.HADOOP_SECURITY;
import static org.apache.flume.configfilter.HadoopCredentialStoreConfigFilter.PASSWORD_FILE_CONFIG_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class TestHadoopCredentialStoreConfigFilter {

  private static String providerPathDefault;
  private static String providerPathEnv;
  private static String providerPathPwdFile;

  @ClassRule
  public static final EnvironmentVariables environmentVariables
      = new EnvironmentVariables();
  private static File fileDefault;
  private static File fileEnvPassword;
  private static File fileFilePassword;
  private HadoopCredentialStoreConfigFilter configFilter;

  @BeforeClass
  public static void setUpClass() throws Exception {
    generateTempFileNames();
    fillCredStoreWithDefaultPassword();
    fillCredStoreWithPasswordFile();
    fillCredStoreWithEnvironmentVariablePassword();
  }
  @AfterClass
  public static void tearDown() {
    fileDefault.deleteOnExit();
    fileEnvPassword.deleteOnExit();
    fileFilePassword.deleteOnExit();
  }

  @Before
  public void setUp() {
    String[] objects = System.getenv().keySet().toArray(new String[0]);
    environmentVariables.clear(objects);
    configFilter = new HadoopCredentialStoreConfigFilter();
  }

  @Test
  public void filterDefaultPasswordFile() {
    HashMap<String, String> configuration = new HashMap<>();
    configuration.put(CREDENTIAL_PROVIDER_PATH, providerPathDefault);
    configFilter.initializeWithConfiguration(configuration);

    assertEquals("filtered_default", configFilter.filter("password"));
  }

  @Test
  public void filterWithEnvPassword() {
    environmentVariables.set("HADOOP_CREDSTORE_PASSWORD","envSecret");
    HashMap<String, String> configuration = new HashMap<>();
    configuration.put(CREDENTIAL_PROVIDER_PATH, providerPathEnv);
    configFilter.initializeWithConfiguration(configuration);

    assertEquals("filtered_env", configFilter.filter("password"));
  }

  @Test
  public void filterWithPasswordFile() {
    HashMap<String, String> configuration = new HashMap<>();
    configuration.put(CREDENTIAL_PROVIDER_PATH, providerPathPwdFile);
    configuration.put(PASSWORD_FILE_CONFIG_KEY, "test-password.txt");
    configFilter.initializeWithConfiguration(configuration);

    assertEquals("filtered_file", configFilter.filter("password"));
  }

  @Test
  public void filterWithEnvNoPassword() {
    HashMap<String, String> configuration = new HashMap<>();
    configuration.put(CREDENTIAL_PROVIDER_PATH, providerPathEnv);
    configFilter.initializeWithConfiguration(configuration);

    assertNull(configFilter.filter("password"));
  }

  @Test
  public void filterErrorWithPasswordFileWrongPassword() {
    HashMap<String, String> configuration = new HashMap<>();
    configuration.put(CREDENTIAL_PROVIDER_PATH, providerPathPwdFile);
    configuration.put(PASSWORD_FILE_CONFIG_KEY, "test-password2.txt");
    configFilter.initializeWithConfiguration(configuration);

    assertNull(configFilter.filter("password"));
  }

  @Test
  public void filterErrorWithPasswordFileNoPasswordFile() {
    HashMap<String, String> configuration = new HashMap<>();
    configuration.put(CREDENTIAL_PROVIDER_PATH, providerPathPwdFile);
    configFilter.initializeWithConfiguration(configuration);

    assertNull(configFilter.filter("password"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void filterErrorWithNoProvider() {
    HashMap<String, String> configuration = new HashMap<>();
    configFilter.initializeWithConfiguration(configuration);
  }


  private static void fillCredStoreWithEnvironmentVariablePassword() throws Exception {
    environmentVariables.set("HADOOP_CREDSTORE_PASSWORD", "envSecret");

    runCommand("create password -value filtered_env -provider "
        + providerPathEnv, new Configuration());
  }

  private static void fillCredStoreWithPasswordFile() throws Exception {
    Configuration conf = new Configuration();
    conf.set(
        HADOOP_SECURITY + PASSWORD_FILE_CONFIG_KEY,
        "test-password.txt"
    );
    runCommand("create password -value filtered_file -provider "
        + providerPathPwdFile, conf);
  }

  private static void fillCredStoreWithDefaultPassword() throws Exception {
    runCommand("create password -value filtered_default -provider "
        + providerPathDefault, new Configuration());
  }

  private static void generateTempFileNames() throws IOException {
    fileDefault = Files.createTempFile("test-default-pwd-", ".jceks").toFile();
    boolean deleted = fileDefault.delete();
    fileEnvPassword = Files.createTempFile("test-env-pwd-", ".jceks").toFile();
    deleted &= fileEnvPassword.delete();
    fileFilePassword = Files.createTempFile("test-file-pwd-", ".jceks").toFile();
    deleted &= fileFilePassword.delete();
    if (!deleted) {
      fail("Could not delete temporary files");
    }

    providerPathDefault = "jceks://file/" + fileDefault.getAbsolutePath();
    providerPathEnv = "jceks://file/" + fileEnvPassword.getAbsolutePath();
    providerPathPwdFile = "jceks://file/" + fileFilePassword.getAbsolutePath();
  }

  private static void runCommand(String c, Configuration conf) throws Exception {
    ToolRunner.run(conf, new CredentialShell(), c.split(" "));
  }


}