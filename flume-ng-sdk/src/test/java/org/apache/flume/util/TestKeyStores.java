/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.util;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.cert.X509Certificate;

/**
 * Test helper that builds key and trust stores backed by a single self-signed certificate,
 * so SSL tests can generate their TLS material in memory instead of shipping keystore files.
 */
public final class TestKeyStores {

    private final KeyPair keyPair;
    private final X509Certificate certificate;

    private TestKeyStores(KeyPair keyPair, X509Certificate certificate) {
        this.keyPair = keyPair;
        this.certificate = certificate;
    }

    /**
     * Generates a self-signed credential for the given distinguished name (for example {@code CN=localhost}).
     */
    public static TestKeyStores selfSigned(String dname) throws Exception {
        KeyPair keyPair = X509Certificates.generateKeyPair();
        X509Certificate certificate = X509Certificates.generateSelfSignedCertificate(keyPair, dname);
        return new TestKeyStores(keyPair, certificate);
    }

    public X509Certificate certificate() {
        return certificate;
    }

    /** A keystore of the given type holding the private key and certificate under alias {@code key}. */
    public KeyStore keyStore(String type, String password) throws Exception {
        KeyStore ks = KeyStore.getInstance(type);
        ks.load(null, null);
        ks.setKeyEntry("key", keyPair.getPrivate(), password.toCharArray(), new X509Certificate[] {certificate});
        return ks;
    }

    /** A truststore of the given type holding only the certificate under alias {@code cert}. */
    public KeyStore trustStore(String type) throws Exception {
        KeyStore ts = KeyStore.getInstance(type);
        ts.load(null, null);
        ts.setCertificateEntry("cert", certificate);
        return ts;
    }

    /** Writes {@link #keyStore} to {@code file} and returns its path. */
    public Path writeKeyStore(Path file, String type, String password) throws Exception {
        KeyStore ks = keyStore(type, password);
        try (OutputStream out = Files.newOutputStream(file)) {
            ks.store(out, password.toCharArray());
        }
        return file;
    }
}
