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

import junit.framework.Assert;

import com.google.common.base.Charsets;

public class CipherProviderTestSuite {

  private final CipherProvider.Encryptor encryptor;
  private final CipherProvider.Decryptor decryptor;

  public CipherProviderTestSuite(CipherProvider.Encryptor encryptor,
      CipherProvider.Decryptor decryptor) {
    this.encryptor = encryptor;
    this.decryptor = decryptor;
  }

  public void test() throws Exception {
    testBasic();
    testEmpty();
    testNullPlainText();
    testNullCipherText();
  }

  public void testBasic() throws Exception {
    String expected = "mn state fair is the place to be";
    byte[] cipherText = encryptor.encrypt(expected.getBytes(Charsets.UTF_8));
    byte[] clearText = decryptor.decrypt(cipherText);
    Assert.assertEquals(expected, new String(clearText, Charsets.UTF_8));
  }

  public void testEmpty() throws Exception {
    String expected = "";
    byte[] cipherText = encryptor.encrypt(new byte[]{});
    byte[] clearText = decryptor.decrypt(cipherText);
    Assert.assertEquals(expected, new String(clearText));
  }

  public void testNullPlainText() throws Exception {
    try {
      encryptor.encrypt(null);
      Assert.fail();
    } catch (NullPointerException e) {
      // expected
    }
  }

  public void testNullCipherText() throws Exception {
    try {
      decryptor.decrypt(null);
      Assert.fail();
    } catch (NullPointerException e) {
      // expected
    }
  }
}
