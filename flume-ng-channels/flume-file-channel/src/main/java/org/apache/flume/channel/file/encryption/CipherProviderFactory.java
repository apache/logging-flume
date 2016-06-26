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

import java.security.Key;
import java.util.Locale;

import org.apache.flume.FlumeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class CipherProviderFactory {
  private static final Logger logger =
      LoggerFactory.getLogger(CipherProviderFactory.class);

  public static CipherProvider.Encryptor getEncrypter(String cipherProviderType,
      Key key) {
    if (cipherProviderType == null) {
      return null;
    }
    CipherProvider provider = getProvider(cipherProviderType);
    return provider.newEncryptorBuilder().setKey(key).build();
  }
  public static CipherProvider.Decryptor getDecrypter(String cipherProviderType,
      Key key, byte[] parameters) {
    if (cipherProviderType == null) {
      return null;
    }
    CipherProvider provider = getProvider(cipherProviderType);
    return provider.newDecryptorBuilder().setKey(key).setParameters(parameters)
        .build();
  }
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static CipherProvider getProvider(String cipherProviderType) {
    Preconditions.checkNotNull(cipherProviderType,
        "cipher provider type must not be null");
    // try to find builder class in enum of known providers
    CipherProviderType type;
    try {
      type = CipherProviderType.valueOf(cipherProviderType.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      logger.debug("Not in enum, loading provider class: {}", cipherProviderType);
      type = CipherProviderType.OTHER;
    }
    Class<? extends CipherProvider> providerClass = type.getProviderClass();

    // handle the case where they have specified their own builder in the config
    if (providerClass == null) {
      try {
        Class c = Class.forName(cipherProviderType);
        if (c != null && CipherProvider.class.isAssignableFrom(c)) {
          providerClass = (Class<? extends CipherProvider>) c;
        } else {
          String errMessage = "Unable to instantiate provider from " +
              cipherProviderType;
          logger.error(errMessage);
          throw new FlumeException(errMessage);
        }
      } catch (ClassNotFoundException ex) {
        logger.error("Class not found: " + cipherProviderType, ex);
        throw new FlumeException(ex);
      }
    }
    try {
      return providerClass.newInstance();
    } catch (Exception ex) {
      String errMessage = "Cannot instantiate provider: " + cipherProviderType;
      logger.error(errMessage, ex);
      throw new FlumeException(errMessage, ex);
    }
  }
}
