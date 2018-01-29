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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public class HadoopCredentialStoreConfigFilter extends AbstractConfigFilter {

  public static final String CREDSTORE_JAVA_KEYSTORE_PROVIDER_PASSWORD_FILE_CONFIG_KEY
      = "credstore.java-keystore-provider.password-file";
  public static final String CREDENTIAL_PROVIDER_PATH
      = "credential.provider.path";
  public static final String HADOOP_SECURITY = "hadoop.security.";
  private Configuration hadoopConfiguration;

  @Override
  protected void initialize() {
    hadoopConfiguration = new Configuration();
    hadoopConfiguration.set(
        HADOOP_SECURITY + CREDENTIAL_PROVIDER_PATH,
        getConfiguration().get(CREDENTIAL_PROVIDER_PATH)
    );

    String passwordFile = getConfiguration()
        .get(CREDSTORE_JAVA_KEYSTORE_PROVIDER_PASSWORD_FILE_CONFIG_KEY);
    if (passwordFile != null && !passwordFile.isEmpty()) {
      hadoopConfiguration.set(
          HADOOP_SECURITY + CREDSTORE_JAVA_KEYSTORE_PROVIDER_PASSWORD_FILE_CONFIG_KEY, passwordFile
      );
    }
  }

  @Override
  public String filter(String key) {
    char[] result;
    try {
      result = hadoopConfiguration.getPassword(key);
    } catch (IOException e) {
      e.printStackTrace();
      result = null;
    }

    return result == null ? null : new String(result);
  }


}
