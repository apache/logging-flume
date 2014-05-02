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

import java.nio.ByteBuffer;
import java.security.Key;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

public class AESCTRNoPaddingProvider extends CipherProvider {
  private static final Logger LOG = LoggerFactory
      .getLogger(AESCTRNoPaddingProvider.class);
  static final String TYPE = "AES/CTR/NoPadding";


  public Encryptor.Builder<AESCTRNoPaddingEncryptor> newEncryptorBuilder() {
    return new EncryptorBuilder();
  }
  public Decryptor.Builder<AESCTRNoPaddingDecryptor> newDecryptorBuilder() {
    return new DecryptorBuilder();
  }

  public static class EncryptorBuilder
    extends CipherProvider.Encryptor.Builder<AESCTRNoPaddingEncryptor> {
    @Override
    public AESCTRNoPaddingEncryptor build() {
      ByteBuffer buffer = ByteBuffer.allocate(16);
      byte[] seed = new byte[12];
      SecureRandom random = new SecureRandom();
      random.nextBytes(seed);
      buffer.put(seed).putInt(1);
      return new AESCTRNoPaddingEncryptor(key, buffer.array());
    }
  }


  public static class DecryptorBuilder
    extends CipherProvider.Decryptor.Builder<AESCTRNoPaddingDecryptor> {
    @Override
    public AESCTRNoPaddingDecryptor build() {
      return new AESCTRNoPaddingDecryptor(key, parameters);
    }
  }

  private static class AESCTRNoPaddingEncryptor extends Encryptor {
    private byte[] parameters;
    private Cipher cipher;
    private AESCTRNoPaddingEncryptor(Key key, byte[] parameters) {
      this.parameters = parameters;
      cipher = getCipher(key, Cipher.ENCRYPT_MODE, parameters);
    }
    @Override
    public byte[] getParameters() {
      return parameters;
    }
    @Override
    public String getCodec() {
      return TYPE;
    }
    @Override
    public byte[] encrypt(byte[] clearText) {
      return doFinal(cipher, clearText);
    }
  }

  private static class AESCTRNoPaddingDecryptor extends Decryptor {
    private Cipher cipher;
    private AESCTRNoPaddingDecryptor(Key key, byte[] parameters) {
      cipher = getCipher(key, Cipher.DECRYPT_MODE, parameters);
    }
    @Override
    public byte[] decrypt(byte[] cipherText) {
      return doFinal(cipher, cipherText);
    }
    @Override
    public String getCodec() {
      return TYPE;
    }
  }

  private static byte[] doFinal(Cipher cipher, byte[] input)
    throws DecryptionFailureException{
    try {
      return cipher.doFinal(input);
    } catch (Exception e) {
      String msg = "Unable to encrypt or decrypt data " + TYPE
          + " input.length " + input.length;
      LOG.error(msg, e);
      throw new DecryptionFailureException(msg, e);
    }
  }

  private static Cipher getCipher(Key key, int mode, byte[] parameters) {
    try {
      Cipher cipher = Cipher.getInstance(TYPE);
      cipher.init(mode, key, new IvParameterSpec(parameters));
      return cipher;
    } catch (Exception e) {
      String msg = "Unable to load key using transformation: " + TYPE;
      if (e instanceof InvalidKeyException) {
        try {
          int maxAllowedLen = Cipher.getMaxAllowedKeyLength(TYPE);
          if (maxAllowedLen < 256) {
            msg += "; Warning: Maximum allowed key length = " + maxAllowedLen
                + " with the available JCE security policy files. Have you"
                + " installed the JCE unlimited strength jurisdiction policy"
                + " files?";
          }
        } catch (NoSuchAlgorithmException ex) {
          msg += "; Unable to find specified algorithm?";
        }
      }
      LOG.error(msg, e);
      throw Throwables.propagate(e);
    }
  }
}
