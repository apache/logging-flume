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

import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

public class TestX509Certificates {

    @Test
    public void testSelfSignedCertificate() throws Exception {
        KeyPair keyPair = X509Certificates.generateKeyPair();
        X509Certificate certificate = X509Certificates.generateSelfSignedCertificate(keyPair, "CN=localhost");

        Assert.assertEquals(
                "CN=localhost", certificate.getSubjectX500Principal().getName());
        Assert.assertEquals(certificate.getSubjectX500Principal(), certificate.getIssuerX500Principal());
        // A self-signed certificate verifies against its own public key.
        certificate.verify(keyPair.getPublic());
        certificate.checkValidity(new Date());
    }

    @Test
    public void testServerCertificateExtensions() throws Exception {
        KeyPair keyPair = X509Certificates.generateKeyPair();
        X509Certificate certificate = X509Certificates.generateSelfSignedCertificate(keyPair, "CN=localhost");

        // A negative path length constraint marks a certificate that is not a CA.
        Assert.assertEquals(-1, certificate.getBasicConstraints());
        Assert.assertEquals(Collections.singletonList("1.3.6.1.5.5.7.3.1"), certificate.getExtendedKeyUsage());
    }

    @Test
    public void testKeyPairsAreDistinct() {
        Assert.assertNotEquals(
                X509Certificates.generateKeyPair().getPrivate(),
                X509Certificates.generateKeyPair().getPrivate());
    }
}
