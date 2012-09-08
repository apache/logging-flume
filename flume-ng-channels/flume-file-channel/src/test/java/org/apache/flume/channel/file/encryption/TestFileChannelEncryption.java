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

import static org.apache.flume.channel.file.TestUtils.*;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.channel.file.FileChannelConfiguration;
import org.apache.flume.channel.file.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class TestFileChannelEncryption {
  protected static final Logger LOGGER =
      LoggerFactory.getLogger(TestFileChannelEncryption.class);
  private FileChannel channel;
  private File baseDir;
  private File checkpointDir;
  private File[] dataDirs;
  private String dataDir;
  private File keyStoreFile;
  private File keyStorePasswordFile;
  private Map<String, File> keyAliasPassword;

  @Before
  public void setup() throws Exception {
    baseDir = Files.createTempDir();
    checkpointDir = new File(baseDir, "chkpt");
    Assert.assertTrue(checkpointDir.mkdirs() || checkpointDir.isDirectory());
    dataDirs = new File[3];
    dataDir = "";
    for (int i = 0; i < dataDirs.length; i++) {
      dataDirs[i] = new File(baseDir, "data" + (i+1));
      Assert.assertTrue(dataDirs[i].mkdirs() || dataDirs[i].isDirectory());
      dataDir += dataDirs[i].getAbsolutePath() + ",";
    }
    dataDir = dataDir.substring(0, dataDir.length() - 1);
    keyStorePasswordFile = new File(baseDir, "keyStorePasswordFile");
    Files.write("keyStorePassword", keyStorePasswordFile, Charsets.UTF_8);
    keyStoreFile = new File(baseDir, "keyStoreFile");
    Assert.assertTrue(keyStoreFile.createNewFile());
    keyAliasPassword = Maps.newHashMap();
    keyAliasPassword.putAll(EncryptionTestUtils.
        configureTestKeyStore(baseDir, keyStoreFile));
  }
  @After
  public void teardown() {
    if(channel != null && channel.isOpen()) {
      channel.stop();
    }
    FileUtils.deleteQuietly(baseDir);
  }
  private Map<String, String> getOverrides() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CAPACITY, String.valueOf(100));
    return overrides;
  }
  private Map<String, String> getOverridesForEncryption() throws Exception {
    Map<String, String> overrides = getOverrides();
    Map<String, String> encryptionProps = EncryptionTestUtils.
        configureForKeyStore(keyStoreFile, keyStorePasswordFile,
            keyAliasPassword);
    encryptionProps.put(EncryptionConfiguration.KEY_PROVIDER,
        KeyProviderType.JCEKSFILE.name());
    encryptionProps.put(EncryptionConfiguration.CIPHER_PROVIDER,
        CipherProviderType.AESCTRNOPADDING.name());
    encryptionProps.put(EncryptionConfiguration.KEY_ALIAS, "key-1");
    for(String key : encryptionProps.keySet()) {
      overrides.put(EncryptionConfiguration.ENCRYPTION_PREFIX + "." + key,
          encryptionProps.get(key));
    }
    return overrides;
  }
  @Test
  public void testBasicEncyrptionDecryption() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
        dataDir, overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = Sets.newHashSet();
    try {
      while(true) {
        in.addAll(putEvents(channel, "restart", 1, 1));
      }
    } catch (ChannelException e) {
      Assert.assertEquals("Cannot acquire capacity. [channel="
          +channel.getName()+"]", e.getMessage());
    }
    channel.stop();
    channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
        dataDir, overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out = takeEvents(channel, 1, Integer.MAX_VALUE);
    compareInputAndOut(in, out);
  }
  @Test
  public void testEncryptedChannelWithoutEncryptionConfigFails() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
        dataDir, overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = Sets.newHashSet();
    try {
      while(true) {
        in.addAll(putEvents(channel, "will-not-restart", 1, 1));
      }
    } catch (ChannelException e) {
      Assert.assertEquals("Cannot acquire capacity. [channel="
          +channel.getName()+"]", e.getMessage());
    }
    channel.stop();
    Map<String, String> noEncryptionOverrides = getOverrides();
    channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
        dataDir, noEncryptionOverrides);
    channel.start();

    if(channel.isOpen()) {
      try {
        takeEvents(channel, 1, 1);
        Assert.fail("Channel was opened and take did not throw exception");
      } catch(ChannelException ex) {
        // expected
      }
    }
  }
  @Test
  public void testUnencyrptedAndEncryptedLogs() throws Exception {
    Map<String, String> noEncryptionOverrides = getOverrides();
    channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
        dataDir, noEncryptionOverrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = Sets.newHashSet();
    try {
      while(true) {
        in.addAll(putEvents(channel, "unencrypted-and-encrypted", 1, 1));
      }
    } catch (ChannelException e) {
      Assert.assertEquals("Cannot acquire capacity. [channel="
          +channel.getName()+"]", e.getMessage());
    }
    int numEventsToRemove = in.size() / 2;
    for (int i = 0; i < numEventsToRemove; i++) {
      Assert.assertTrue(in.removeAll(takeEvents(channel, 1, 1)));
    }
    // now we have logs with no encryption and the channel is half full
    channel.stop();
    Map<String, String> overrides = getOverridesForEncryption();
    channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
        dataDir, overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    try {
      while(true) {
        in.addAll(putEvents(channel, "unencrypted-and-encrypted", 1, 1));
      }
    } catch (ChannelException e) {
      Assert.assertEquals("Cannot acquire capacity. [channel="
          +channel.getName()+"]", e.getMessage());
    }
    Set<String> out = takeEvents(channel, 1, Integer.MAX_VALUE);
    compareInputAndOut(in, out);
  }
  @Test
  public void testBadKeyProviderInvalidValue() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(EncryptionConfiguration.ENCRYPTION_PREFIX + "." +
        EncryptionConfiguration.KEY_PROVIDER, "invalid");
    try {
      channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
          dataDir, overrides);
      Assert.fail();
    } catch(FlumeException ex) {
      Assert.assertEquals("java.lang.ClassNotFoundException: invalid",
          ex.getMessage());
    }
  }
  @Test
  public void testBadKeyProviderInvalidClass() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(EncryptionConfiguration.ENCRYPTION_PREFIX + "." +
        EncryptionConfiguration.KEY_PROVIDER, String.class.getName());
    try {
      channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
          dataDir, overrides);
      Assert.fail();
    } catch(FlumeException ex) {
      Assert.assertEquals("Unable to instantiate Builder from java.lang.String",
          ex.getMessage());
    }
  }
  @Test
  public void testBadCipherProviderInvalidValue() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(EncryptionConfiguration.ENCRYPTION_PREFIX + "." +
        EncryptionConfiguration.CIPHER_PROVIDER, "invalid");
    channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
        dataDir, overrides);
    channel.start();
    Assert.assertFalse(channel.isOpen());
  }
  @Test
  public void testBadCipherProviderInvalidClass() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(EncryptionConfiguration.ENCRYPTION_PREFIX + "." +
        EncryptionConfiguration.CIPHER_PROVIDER, String.class.getName());
    channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
        dataDir, overrides);
    channel.start();
    Assert.assertFalse(channel.isOpen());
  }
  @Test
  public void testMissingKeyStoreFile() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(EncryptionConfiguration.ENCRYPTION_PREFIX + "." +
        EncryptionConfiguration.JCE_FILE_KEY_STORE_FILE, "/path/does/not/exist");
    try {
      channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
          dataDir, overrides);
      Assert.fail();
    } catch(RuntimeException ex) {
      Assert.assertEquals("java.io.FileNotFoundException: /path/does/not/exist " +
          "(No such file or directory)", ex.getMessage());
    }
  }
  @Test
  public void testMissingKeyStorePasswordFile() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(EncryptionConfiguration.ENCRYPTION_PREFIX + "." +
        EncryptionConfiguration.JCE_FILE_KEY_STORE_PASSWORD_FILE, "/path/does/not/exist");
    try {
      channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
          dataDir, overrides);
      Assert.fail();
    } catch(RuntimeException ex) {
      Assert.assertEquals("java.io.FileNotFoundException: /path/does/not/exist " +
          "(No such file or directory)", ex.getMessage());
    }
  }
  @Test
  public void testBadKeyStorePassword() throws Exception {
    Files.write("invalid", keyStorePasswordFile, Charsets.UTF_8);
    Map<String, String> overrides = getOverridesForEncryption();
    try {
      channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
          dataDir, overrides);
      Assert.fail();
    } catch(RuntimeException ex) {
      Assert.assertEquals("java.io.IOException: Keystore was tampered with, or " +
          "password was incorrect", ex.getMessage());
    }
  }
  @Test
  public void testBadKeyAlias() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(EncryptionConfiguration.ENCRYPTION_PREFIX + "." +
        EncryptionConfiguration.KEY_ALIAS, "invalid");
    channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
        dataDir, overrides);
    channel.start();
    Assert.assertFalse(channel.isOpen());
  }
}