/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.channel.file.encryption;

import java.io.File;
import java.io.FileInputStream;
import java.security.Key;
import java.security.KeyStore;
import java.util.Map;

import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

public class JCEFileKeyProvider extends KeyProvider {
  private static final Logger logger =
      LoggerFactory.getLogger(JCEFileKeyProvider.class);

  private Map<String, File> aliasPasswordFileMap;
  private KeyStore ks;
  private char[] keyStorePassword;
  private File keyStorePasswordFile;

  public JCEFileKeyProvider(File keyStoreFile, File keyStorePasswordFile,
      Map<String, File> aliasPasswordFileMap) {
    super();
    this.aliasPasswordFileMap = aliasPasswordFileMap;
    this.keyStorePasswordFile = keyStorePasswordFile;
    try {
      ks = KeyStore.getInstance("jceks");
      keyStorePassword = Files.toString(keyStorePasswordFile, Charsets.UTF_8)
          .trim().toCharArray();
      ks.load(new FileInputStream(keyStoreFile), keyStorePassword);
    } catch(Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  @Override
  public Key getKey(String alias) {
    String passwordFile = keyStorePasswordFile.getAbsolutePath();
    try {
      char[] keyPassword = keyStorePassword;
      if(aliasPasswordFileMap.containsKey(alias)) {
        File keyPasswordFile = aliasPasswordFileMap.get(alias);
        keyPassword = Files.toString(keyPasswordFile,
            Charsets.UTF_8).trim().toCharArray();
        passwordFile = keyPasswordFile.getAbsolutePath();
      }
      Key key = ks.getKey(alias, keyPassword);
      if(key == null) {
        throw new IllegalStateException("KeyStore returned null for " + alias);
      }
      return key;
    } catch (Exception e) {
      String msg = e.getClass().getName() + ": " + e.getMessage() + ". " +
          "Key = " + alias + ", passwordFile = " + passwordFile;
      throw new RuntimeException(msg, e);
    }
  }

  public static class Builder implements KeyProvider.Builder {
    @Override
    public KeyProvider build(Context context) {
      String keyStoreFileName = context.getString(
          EncryptionConfiguration.JCE_FILE_KEY_STORE_FILE);
      String keyStorePasswordFileName = context.getString(
          EncryptionConfiguration.JCE_FILE_KEY_STORE_PASSWORD_FILE);
      Preconditions.checkState(!Strings.isNullOrEmpty(keyStoreFileName),
          "KeyStore file not specified");
      Preconditions.checkState(!Strings.isNullOrEmpty(keyStorePasswordFileName),
          "KeyStore password  file not specified");
      Map<String, File> aliasPasswordFileMap = Maps.newHashMap();
      String passwordProtectedKeys = context.getString(
          EncryptionConfiguration.JCE_FILE_KEYS);
      Preconditions.checkState(!Strings.isNullOrEmpty(passwordProtectedKeys),
          "Keys available to KeyStore was not specified or empty");
      for(String passwordName : passwordProtectedKeys.trim().split("\\s+")) {
        String propertyName = Joiner.on(".").join(EncryptionConfiguration.JCE_FILE_KEYS,
            passwordName, EncryptionConfiguration.JCE_FILE_KEY_PASSWORD_FILE);
        String passwordFileName = context.getString(propertyName,
            keyStorePasswordFileName);
        File passwordFile = new File(passwordFileName.trim());
        if(passwordFile.isFile()) {
          aliasPasswordFileMap.put(passwordName, passwordFile);
        } else {
          logger.warn("Password file for alias " + passwordName +
              " does not exist");
        }
      }
      File keyStoreFile = new File(keyStoreFileName.trim());
      File keyStorePasswordFile = new File(keyStorePasswordFileName.trim());
      return new JCEFileKeyProvider(keyStoreFile, keyStorePasswordFile,
          aliasPasswordFileMap);
    }
  }
}
