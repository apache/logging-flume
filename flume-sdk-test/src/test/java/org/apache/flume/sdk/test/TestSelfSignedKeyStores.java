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
package org.apache.flume.sdk.test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import org.junit.Assert;
import org.junit.Test;

public class TestSelfSignedKeyStores {

    private static final String PASSWORD = "password";

    @Test
    public void testKeyStoreHoldsThePrivateKey() throws Exception {
        TestKeyStores credentials = TestKeyStores.selfSigned("CN=localhost");
        KeyStore keyStore = credentials.keyStore("JKS", PASSWORD);

        Assert.assertTrue(keyStore.isKeyEntry("key"));
        Assert.assertEquals(credentials.certificate(), keyStore.getCertificateChain("key")[0]);
        Assert.assertNotNull(keyStore.getKey("key", PASSWORD.toCharArray()));
    }

    @Test
    public void testTrustStoreHoldsOnlyTheCertificate() throws Exception {
        TestKeyStores credentials = TestKeyStores.selfSigned("CN=localhost");
        KeyStore trustStore = credentials.trustStore("JKS");

        Assert.assertFalse(trustStore.isKeyEntry("cert"));
        Assert.assertEquals(credentials.certificate(), trustStore.getCertificate("cert"));
    }

    @Test
    public void testWrittenKeyStoreCanBeReloaded() throws Exception {
        TestKeyStores credentials = TestKeyStores.selfSigned("CN=localhost");
        Path file = Files.createTempFile("keystore", ".jks");
        try {
            credentials.writeKeyStore(file, "JKS", PASSWORD);

            KeyStore reloaded = KeyStore.getInstance("JKS");
            try (InputStream in = Files.newInputStream(file)) {
                reloaded.load(in, PASSWORD.toCharArray());
            }
            X509Certificate certificate = (X509Certificate) reloaded.getCertificateChain("key")[0];
            Assert.assertEquals(credentials.certificate(), certificate);
        } finally {
            Files.deleteIfExists(file);
        }
    }
}
