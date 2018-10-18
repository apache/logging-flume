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

package org.apache.flume.source;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.util.SSLUtil;

public abstract class SslContextAwareAbstractSource extends AbstractSource {
  private static final String SSL_ENABLED_KEY = "ssl";
  private static final boolean SSL_ENABLED_DEFAULT_VALUE = false;
  private static final String KEYSTORE_KEY = "keystore";
  private static final String KEYSTORE_PASSWORD_KEY = "keystore-password";
  private static final String KEYSTORE_TYPE_KEY = "keystore-type";
  private static final String KEYSTORE_TYPE_DEFAULT_VALUE = "JKS";

  private static final String EXCLUDE_PROTOCOLS = "exclude-protocols";
  private static final String INCLUDE_PROTOCOLS = "include-protocols";

  private static final String EXCLUDE_CIPHER_SUITES = "exclude-cipher-suites";
  private static final String INCLUDE_CIPHER_SUITES = "include-cipher-suites";

  private String keystore;
  private String keystorePassword;
  private String keystoreType;
  private boolean sslEnabled = false;
  private final Set<String> excludeProtocols = new LinkedHashSet<>(Arrays.asList("SSLv3"));
  private final Set<String> includeProtocols = new LinkedHashSet<>();
  private final Set<String> excludeCipherSuites = new LinkedHashSet<>();
  private final Set<String> includeCipherSuites = new LinkedHashSet<>();


  public String getKeystore() {
    return keystore;
  }

  public String getKeystorePassword() {
    return keystorePassword;
  }

  public String getKeystoreType() {
    return keystoreType;
  }

  public Set<String> getExcludeProtocols() {
    return excludeProtocols;
  }

  public Set<String> getIncludeProtocols() {
    return includeProtocols;
  }

  public Set<String> getExcludeCipherSuites() {
    return excludeCipherSuites;
  }

  public Set<String> getIncludeCipherSuites() {
    return includeCipherSuites;
  }

  public boolean isSslEnabled() {
    return sslEnabled;
  }

  protected void configureSsl(Context context) {
    sslEnabled = context.getBoolean(SSL_ENABLED_KEY, SSL_ENABLED_DEFAULT_VALUE);
    keystore = context.getString(KEYSTORE_KEY, SSLUtil.getGlobalKeystorePath());
    keystorePassword = context.getString(
        KEYSTORE_PASSWORD_KEY, SSLUtil.getGlobalKeystorePassword());
    keystoreType = context.getString(
        KEYSTORE_TYPE_KEY, SSLUtil.getGlobalKeystoreType(KEYSTORE_TYPE_DEFAULT_VALUE));

    parseList(context.getString(EXCLUDE_PROTOCOLS, SSLUtil.getGlobalExcludeProtocols()),
        excludeProtocols);
    parseList(context.getString(INCLUDE_PROTOCOLS, SSLUtil.getGlobalIncludeProtocols()),
        includeProtocols);
    parseList(context.getString(EXCLUDE_CIPHER_SUITES, SSLUtil.getGlobalExcludeCipherSuites()),
        excludeCipherSuites);
    parseList(context.getString(INCLUDE_CIPHER_SUITES, SSLUtil.getGlobalIncludeCipherSuites()),
        includeCipherSuites);

    if (sslEnabled) {
      Objects.requireNonNull(keystore,
          KEYSTORE_KEY + " must be specified when SSL is enabled");
      Objects.requireNonNull(keystorePassword,
          KEYSTORE_PASSWORD_KEY + " must be specified when SSL is enabled");
      try {
        KeyStore ks = KeyStore.getInstance(keystoreType);
        ks.load(new FileInputStream(keystore), keystorePassword.toCharArray());
      } catch (Exception ex) {
        throw new FlumeException(
          "Source " + getName() + " configured with invalid keystore: " + keystore, ex);
      }
    }
  }

  private Optional<SSLContext> getSslContext() {
    if (sslEnabled) {
      try {
        KeyStore ks = KeyStore.getInstance(keystoreType);
        ks.load(new FileInputStream(keystore), keystorePassword.toCharArray());

        // can be set with "ssl.KeyManagerFactory.algorithm"
        String algorithm = KeyManagerFactory.getDefaultAlgorithm();
        // Set up key manager factory to use our key store
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
        kmf.init(ks, keystorePassword.toCharArray());

        SSLContext serverContext = SSLContext.getInstance("TLS");
        serverContext.init(kmf.getKeyManagers(), null, null);

        return Optional.of(serverContext);
      } catch (Exception e) {
        throw new Error("Failed to initialize the server-side SSLContext", e);
      }
    } else {
      return Optional.empty();
    }
  }

  private Optional<SSLEngine> getSslEngine(boolean useClientMode) {
    return getSslContext().map(sslContext -> {
      SSLEngine sslEngine = sslContext.createSSLEngine();
      sslEngine.setUseClientMode(useClientMode);
      sslEngine.setEnabledProtocols(
          getFilteredProtocols(sslEngine.getEnabledProtocols()));
      sslEngine.setEnabledCipherSuites(
          getFilteredCipherSuites(sslEngine.getEnabledCipherSuites()));
      return sslEngine;
    });
  }

  protected Supplier<Optional<SSLContext>> getSslContextSupplier() {
    return this::getSslContext;
  }


  protected Supplier<Optional<SSLEngine>> getSslEngineSupplier(boolean useClientMode) {
    return () -> getSslEngine(useClientMode);
  }

  protected String[] getFilteredProtocols(SSLParameters sslParameters) {
    return getFilteredProtocols(sslParameters.getProtocols());
  }

  private String[] getFilteredProtocols(String[] enabledProtocols) {
    return Stream.of(enabledProtocols)
      .filter(o -> includeProtocols.isEmpty() || includeProtocols.contains(o))
      .filter(o -> !excludeProtocols.contains(o) )
      .toArray(String[]::new);
  }

  protected String[] getFilteredCipherSuites(SSLParameters sslParameters) {
    return getFilteredCipherSuites(sslParameters.getCipherSuites());
  }

  private String[] getFilteredCipherSuites(String[] enabledCipherSuites) {
    return Stream.of(enabledCipherSuites)
      .filter(o -> includeCipherSuites.isEmpty() || includeCipherSuites.contains(o))
      .filter(o -> !excludeCipherSuites.contains(o))
      .toArray(String[]::new);
  }

  private void parseList(String value, Set<String> set) {
    if (Objects.nonNull(value)) {
      set.addAll(Arrays.asList(value.split(" ")));
    }
  }
}
