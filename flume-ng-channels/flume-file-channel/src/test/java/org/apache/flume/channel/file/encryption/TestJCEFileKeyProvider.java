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

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.apache.flume.Context;
import org.apache.flume.channel.file.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.security.Key;
import java.util.Map;

public class TestJCEFileKeyProvider {
  private CipherProvider.Encryptor encryptor;
  private CipherProvider.Decryptor decryptor;
  private File baseDir;
  private File keyStoreFile;
  private File keyStorePasswordFile;
  private Map<String, File> keyAliasPassword;

  @Before
  public void setup() throws Exception {
    baseDir = Files.createTempDir();
    keyStorePasswordFile = new File(baseDir, "keyStorePasswordFile");
    Files.write("keyStorePassword", keyStorePasswordFile, Charsets.UTF_8);
    keyAliasPassword = Maps.newHashMap();
    keyStoreFile = new File(baseDir, "keyStoreFile");
    Assert.assertTrue(keyStoreFile.createNewFile());

  }

  @After
  public void cleanup() {
    FileUtils.deleteQuietly(baseDir);
  }

  private void initializeForKey(Key key) {
    encryptor = new AESCTRNoPaddingProvider.EncryptorBuilder()
                                           .setKey(key)
                                           .build();
    decryptor = new AESCTRNoPaddingProvider.DecryptorBuilder()
                                           .setKey(key)
                                           .setParameters(encryptor.getParameters())
                                           .build();
  }

  @Test
  public void testWithNewKeyStore() throws Exception {
    createNewKeyStore();
    EncryptionTestUtils.createKeyStore(keyStoreFile, keyStorePasswordFile,
        keyAliasPassword);
    Context context = new Context(
        EncryptionTestUtils.configureForKeyStore(keyStoreFile,
                                                 keyStorePasswordFile,
                                                 keyAliasPassword));
    Context keyProviderContext = new Context(
        context.getSubProperties(EncryptionConfiguration.KEY_PROVIDER + "."));
    KeyProvider keyProvider =
        KeyProviderFactory.getInstance(KeyProviderType.JCEKSFILE.name(), keyProviderContext);
    testKeyProvider(keyProvider);
  }

  @Test
  public void testWithExistingKeyStore() throws Exception {
    keyAliasPassword.putAll(EncryptionTestUtils.configureTestKeyStore(baseDir, keyStoreFile));
    Context context = new Context(
        EncryptionTestUtils.configureForKeyStore(keyStoreFile,
                                                 keyStorePasswordFile,
                                                 keyAliasPassword));
    Context keyProviderContext = new Context(
        context.getSubProperties(EncryptionConfiguration.KEY_PROVIDER + "."));
    KeyProvider keyProvider =
        KeyProviderFactory.getInstance(KeyProviderType.JCEKSFILE.name(), keyProviderContext);
    testKeyProvider(keyProvider);
  }

  private void createNewKeyStore() throws Exception {
    for (int i = 0; i < 10; i++) {
      // create some with passwords, some without
      if (i % 2 == 0) {
        String alias = "test-" + i;
        String password = String.valueOf(i);
        keyAliasPassword.put(alias, TestUtils.writeStringToFile(baseDir, alias, password));
      }
    }
  }

  private void testKeyProvider(KeyProvider keyProvider) {
    for (String alias : keyAliasPassword.keySet()) {
      Key key = keyProvider.getKey(alias);
      initializeForKey(key);
      String expected = "some text here " + alias;
      byte[] cipherText = encryptor.encrypt(expected.getBytes(Charsets.UTF_8));
      byte[] clearText = decryptor.decrypt(cipherText);
      Assert.assertEquals(expected, new String(clearText, Charsets.UTF_8));
    }
  }
}
