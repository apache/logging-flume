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
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.ChannelException;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.file.FileChannelConfiguration;
import org.apache.flume.channel.file.TestFileChannelBase;
import org.apache.flume.channel.file.TestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

public class TestFileChannelEncryption extends TestFileChannelBase {
  protected static final Logger LOGGER =
      LoggerFactory.getLogger(TestFileChannelEncryption.class);
  private File keyStoreFile;
  private File keyStorePasswordFile;
  private Map<String, File> keyAliasPassword;

  @Before
  public void setup() throws Exception {
    super.setup();
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
    super.teardown();
  }
  private Map<String, String> getOverrides() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put(FileChannelConfiguration.CAPACITY, String.valueOf(100));
    overrides.put(FileChannelConfiguration.TRANSACTION_CAPACITY,
        String.valueOf(100));
    return overrides;
  }
  private Map<String, String> getOverridesForEncryption() throws Exception {
    Map<String, String> overrides = getOverrides();
    Map<String, String> encryptionProps = EncryptionTestUtils.
        configureForKeyStore(keyStoreFile,
            keyStorePasswordFile, keyAliasPassword);
    encryptionProps.put(EncryptionConfiguration.KEY_PROVIDER,
        KeyProviderType.JCEKSFILE.name());
    encryptionProps.put(EncryptionConfiguration.CIPHER_PROVIDER,
        CipherProviderType.AESCTRNOPADDING.name());
    encryptionProps.put(EncryptionConfiguration.ACTIVE_KEY, "key-1");
    for(String key : encryptionProps.keySet()) {
      overrides.put(EncryptionConfiguration.ENCRYPTION_PREFIX + "." + key,
          encryptionProps.get(key));
    }
    return overrides;
  }
  /**
   * Test fails without FLUME-1565
   */
  @Test
  public void testThreadedConsume() throws Exception {
    int numThreads = 20;
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(FileChannelConfiguration.CAPACITY, String.valueOf(10000));
    overrides.put(FileChannelConfiguration.TRANSACTION_CAPACITY,
        String.valueOf(100));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Executor executor = Executors.newFixedThreadPool(numThreads);
    Set<String> in = fillChannel(channel, "threaded-consume");
    final AtomicBoolean error = new AtomicBoolean(false);
    final CountDownLatch startLatch = new CountDownLatch(numThreads);
    final CountDownLatch stopLatch = new CountDownLatch(numThreads);
    final Set<String> out = Collections.synchronizedSet(new HashSet<String>());
    for (int i = 0; i < numThreads; i++) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            startLatch.countDown();
            startLatch.await();
            out.addAll(takeEvents(channel, 10));
          } catch (Throwable t) {
            error.set(true);
            LOGGER.error("Error in take thread", t);
          } finally {
            stopLatch.countDown();
          }
        }
      });
    }
    stopLatch.await();
    Assert.assertFalse(error.get());
    compareInputAndOut(in, out);
  }
  @Test
  public void testThreadedProduce() throws Exception {
    int numThreads = 20;
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(FileChannelConfiguration.CAPACITY, String.valueOf(10000));
    overrides.put(FileChannelConfiguration.TRANSACTION_CAPACITY,
        String.valueOf(100));
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Executor executor = Executors.newFixedThreadPool(numThreads);
    final AtomicBoolean error = new AtomicBoolean(false);
    final CountDownLatch startLatch = new CountDownLatch(numThreads);
    final CountDownLatch stopLatch = new CountDownLatch(numThreads);
    final Set<String> in = Collections.synchronizedSet(new HashSet<String>());
    for (int i = 0; i < numThreads; i++) {
      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            startLatch.countDown();
            startLatch.await();
            in.addAll(putEvents(channel, "thread-produce", 10, 10000, true));
          } catch (Throwable t) {
            error.set(true);
            LOGGER.error("Error in put thread", t);
          } finally {
            stopLatch.countDown();
          }
        }
      });
    }
    stopLatch.await();
    Set<String> out = consumeChannel(channel);

    Assert.assertFalse(error.get());
    compareInputAndOut(in, out);
  }
  @Test
  public void testConfiguration() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("encryption.activeKey", "key-1");
    overrides.put("encryption.cipherProvider", "AESCTRNOPADDING");
    overrides.put("encryption.keyProvider", "JCEKSFILE");
    overrides.put("encryption.keyProvider.keyStoreFile",
        keyStoreFile.getAbsolutePath());
    overrides.put("encryption.keyProvider.keyStorePasswordFile",
        keyStorePasswordFile.getAbsolutePath());
    overrides.put("encryption.keyProvider.keys", "key-0 key-1");
    overrides.put("encryption.keyProvider.keys.key-0.passwordFile",
        keyAliasPassword.get("key-0").getAbsolutePath());
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = fillChannel(channel, "restart");
    channel.stop();
    channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
        dataDir, overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out =  consumeChannel(channel);
    compareInputAndOut(in, out);
  }
  @Test
  public void testBasicEncyrptionDecryption() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = fillChannel(channel, "restart");
    channel.stop();
    channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
        dataDir, overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> out =  consumeChannel(channel);
    compareInputAndOut(in, out);
  }
  @Test
  public void testEncryptedChannelWithoutEncryptionConfigFails() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    fillChannel(channel, "will-not-restart");
    channel.stop();
    Map<String, String> noEncryptionOverrides = getOverrides();
    channel = createFileChannel(noEncryptionOverrides);
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
    channel = createFileChannel(noEncryptionOverrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    Set<String> in = fillChannel(channel, "unencrypted-and-encrypted");
    int numEventsToRemove = in.size() / 2;
    for (int i = 0; i < numEventsToRemove; i++) {
      Assert.assertTrue(in.removeAll(takeEvents(channel, 1, 1)));
    }
    // now we have logs with no encryption and the channel is half full
    channel.stop();
    Map<String, String> overrides = getOverridesForEncryption();
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertTrue(channel.isOpen());
    in.addAll(fillChannel(channel, "unencrypted-and-encrypted"));
    Set<String> out = consumeChannel(channel);
    compareInputAndOut(in, out);
  }
  @Test
  public void testBadKeyProviderInvalidValue() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(Joiner.on(".").join(EncryptionConfiguration.ENCRYPTION_PREFIX,
        EncryptionConfiguration.KEY_PROVIDER), "invalid");
    try {
      channel = createFileChannel(overrides);
      Assert.fail();
    } catch(FlumeException ex) {
      Assert.assertEquals("java.lang.ClassNotFoundException: invalid",
          ex.getMessage());
    }
  }
  @Test
  public void testBadKeyProviderInvalidClass() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(Joiner.on(".").join(EncryptionConfiguration.ENCRYPTION_PREFIX,
        EncryptionConfiguration.KEY_PROVIDER), String.class.getName());
    try {
      channel = createFileChannel(overrides);
      Assert.fail();
    } catch(FlumeException ex) {
      Assert.assertEquals("Unable to instantiate Builder from java.lang.String",
          ex.getMessage());
    }
  }
  @Test
  public void testBadCipherProviderInvalidValue() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(Joiner.on(".").join(EncryptionConfiguration.ENCRYPTION_PREFIX,
        EncryptionConfiguration.CIPHER_PROVIDER), "invalid");
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertFalse(channel.isOpen());
  }
  @Test
  public void testBadCipherProviderInvalidClass() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(Joiner.on(".").join(EncryptionConfiguration.ENCRYPTION_PREFIX,
        EncryptionConfiguration.CIPHER_PROVIDER), String.class.getName());
    channel = createFileChannel(overrides);
    channel.start();
    Assert.assertFalse(channel.isOpen());
  }
  @Test
  public void testMissingKeyStoreFile() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(Joiner.on(".").join(EncryptionConfiguration.ENCRYPTION_PREFIX,
        EncryptionConfiguration.KEY_PROVIDER,
        EncryptionConfiguration.JCE_FILE_KEY_STORE_FILE),
        "/path/does/not/exist");
    try {
      channel = createFileChannel(overrides);
      Assert.fail();
    } catch(RuntimeException ex) {
      Assert.assertEquals("java.io.FileNotFoundException: /path/does/not/exist " +
          "(No such file or directory)", ex.getMessage());
    }
  }
  @Test
  public void testMissingKeyStorePasswordFile() throws Exception {
    Map<String, String> overrides = getOverridesForEncryption();
    overrides.put(Joiner.on(".").join(EncryptionConfiguration.ENCRYPTION_PREFIX,
        EncryptionConfiguration.KEY_PROVIDER,
        EncryptionConfiguration.JCE_FILE_KEY_STORE_PASSWORD_FILE),
        "/path/does/not/exist");
    try {
      channel = createFileChannel(overrides);
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
        EncryptionConfiguration.ACTIVE_KEY, "invalid");
    channel = TestUtils.createFileChannel(checkpointDir.getAbsolutePath(),
        dataDir, overrides);
    channel.start();
    Assert.assertFalse(channel.isOpen());
  }
}