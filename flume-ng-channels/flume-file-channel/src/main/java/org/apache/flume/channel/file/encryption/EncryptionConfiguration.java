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

public class EncryptionConfiguration {
  private EncryptionConfiguration() {}

  // prefix before all encryption options
  public static final String ENCRYPTION_PREFIX = "encryption";
  /**
   * Encryption key provider, default is null.
   */
  public static final String KEY_PROVIDER = "keyProvider";
  /**
   * Encryption key alias, default is null.
   */
  public static final String ACTIVE_KEY = "activeKey";
  /**
   * Encryption cipher provider, default is null.
   */
  public static final String CIPHER_PROVIDER = "cipherProvider";

  /**
   * Space separated list of keys which are needed for the current set of logs
   * plus the one specified in keyAlias
   */
  public static final String JCE_FILE_KEYS = "keys";
  /**
   * Path to key password file is:
   * keys.aliasName.passwordFile
   */
  public static final String JCE_FILE_KEY_PASSWORD_FILE = "passwordFile";

  /**
   * Path to a jceks key store
   */
  public static final String JCE_FILE_KEY_STORE_FILE = "keyStoreFile";
  /**
   * Path to a jceks key store password file
   */
  public static final String JCE_FILE_KEY_STORE_PASSWORD_FILE = "keyStorePasswordFile";

}
