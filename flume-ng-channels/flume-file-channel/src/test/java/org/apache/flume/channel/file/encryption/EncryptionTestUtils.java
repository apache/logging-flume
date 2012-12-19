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
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.Key;
import java.security.KeyStore;
import java.util.List;
import java.util.Map;

import javax.crypto.KeyGenerator;

import org.apache.flume.channel.file.TestUtils;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import com.google.common.io.Resources;

public class EncryptionTestUtils {

  private static Key newKey() {
    KeyGenerator keyGen;
    try {
      keyGen = KeyGenerator.getInstance("AES");
      Key key = keyGen.generateKey();
      return key;
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
  public static void createKeyStore(File keyStoreFile,
      File keyStorePasswordFile, Map<String, File> keyAliasPassword)
          throws Exception {
    KeyStore ks = KeyStore.getInstance("jceks");
    ks.load(null);
    List<String> keysWithSeperatePasswords = Lists.newArrayList();
    for(String alias : keyAliasPassword.keySet()) {
      Key key = newKey();
      char[] password = null;
      File passwordFile = keyAliasPassword.get(alias);
      if(passwordFile == null) {
        password = Files.toString(keyStorePasswordFile, Charsets.UTF_8)
            .toCharArray();
      } else {
        keysWithSeperatePasswords.add(alias);
        password = Files.toString(passwordFile, Charsets.UTF_8).toCharArray();
      }
      ks.setKeyEntry(alias, key, password, null);
    }
    char[] keyStorePassword = Files.
        toString(keyStorePasswordFile, Charsets.UTF_8).toCharArray();
    FileOutputStream outputStream = new FileOutputStream(keyStoreFile);
    ks.store(outputStream, keyStorePassword);
    outputStream.close();
  }
  public static Map<String, File> configureTestKeyStore(File baseDir,
      File keyStoreFile) throws IOException {
    Map<String, File> result = Maps.newHashMap();

    if (System.getProperty("java.vendor").contains("IBM")) {
      Resources.copy(Resources.getResource("ibm-test.keystore"),
          new FileOutputStream(keyStoreFile));
    } else {
      Resources.copy(Resources.getResource("sun-test.keystore"),
          new FileOutputStream(keyStoreFile));
    }
    /*
    Commands below:
    keytool -genseckey -alias key-0 -keypass keyPassword -keyalg AES \
      -keysize 128 -validity 9000 -keystore src/test/resources/test.keystore \
      -storetype jceks -storepass keyStorePassword
    keytool -genseckey -alias key-1 -keyalg AES -keysize 128 -validity 9000 \
      -keystore src/test/resources/test.keystore -storetype jceks \
      -storepass keyStorePassword
     */
//  key-0 has own password, key-1 used key store password
    result.put("key-0",
        TestUtils.writeStringToFile(baseDir, "key-0", "keyPassword"));
    result.put("key-1", null);
    return result;
  }
  public static Map<String,String> configureForKeyStore(File keyStoreFile,
      File keyStorePasswordFile, Map<String, File> keyAliasPassword)
          throws Exception {
    Map<String, String> context = Maps.newHashMap();
    List<String> keys = Lists.newArrayList();
    Joiner joiner = Joiner.on(".");
    for(String alias : keyAliasPassword.keySet()) {
      File passwordFile = keyAliasPassword.get(alias);
      if(passwordFile == null) {
        keys.add(alias);
      } else {
        String propertyName = joiner.join(EncryptionConfiguration.KEY_PROVIDER,
            EncryptionConfiguration.JCE_FILE_KEYS, alias,
            EncryptionConfiguration.JCE_FILE_KEY_PASSWORD_FILE);
        keys.add(alias);
        context.put(propertyName, passwordFile.getAbsolutePath());
      }
    }
    context.put(joiner.join(EncryptionConfiguration.KEY_PROVIDER,
        EncryptionConfiguration.JCE_FILE_KEY_STORE_FILE),
        keyStoreFile.getAbsolutePath());
    if(keyStorePasswordFile != null) {
      context.put(joiner.join(EncryptionConfiguration.KEY_PROVIDER,
          EncryptionConfiguration.JCE_FILE_KEY_STORE_PASSWORD_FILE),
          keyStorePasswordFile.getAbsolutePath());
    }
    context.put(joiner.join(EncryptionConfiguration.KEY_PROVIDER,
        EncryptionConfiguration.JCE_FILE_KEYS),
        Joiner.on(" ").join(keys));
    return context;
  }
}
