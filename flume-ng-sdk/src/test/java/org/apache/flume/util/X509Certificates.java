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

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.Random;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.ExtendedKeyUsage;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.KeyPurposeId;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * Utility class to generate X.509 certificates for testing purposes.
 */
public final class X509Certificates {

    private static final long MINUTE_IN_MILLIS = 60_000L;
    private static final long YEAR_IN_MILLIS = 365L * 24 * 60 * MINUTE_IN_MILLIS;

    private static final KeyPairGenerator RSA_GENERATOR;
    private static final Random RANDOM = new Random();

    static {
        try {
            RSA_GENERATOR = KeyPairGenerator.getInstance("RSA");
            RSA_GENERATOR.initialize(2048);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static KeyPair generateKeyPair() {
        return RSA_GENERATOR.generateKeyPair();
    }

    /**
     * Create a self-signed X.509 server certificate for tests.
     *
     * @param keyPair the certificate key pair
     * @param subjectDn the subject distinguished name (for example {@code CN=localhost})
     * @return a self-signed X.509 server certificate
     * @throws Exception if certificate creation or signing fails
     */
    public static X509Certificate generateSelfSignedCertificate(KeyPair keyPair, String subjectDn) throws Exception {
        long now = System.currentTimeMillis();
        Date notBefore = new Date(now - MINUTE_IN_MILLIS);
        Date notAfter = new Date(now + YEAR_IN_MILLIS);
        BigInteger serial = BigInteger.valueOf(RANDOM.nextLong()).abs();

        X500Name dn = new X500Name(subjectDn);
        JcaX509v3CertificateBuilder builder =
                new JcaX509v3CertificateBuilder(dn, serial, notBefore, notAfter, dn, keyPair.getPublic());

        builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(false));
        // The required key usage for the server certificate depends on the key exchange algorithm:
        // - keyEncipherment for RSA key exchange (deprecated)
        // - digitalSignature for ephemeral Diffie-Hellman key exchange (DHE or ECDHE)
        // - keyAgreement for static Diffie-Hellman key exchange (DH or ECDH)
        builder.addExtension(Extension.keyUsage, true, new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyAgreement));
        builder.addExtension(Extension.extendedKeyUsage, false, new ExtendedKeyUsage(KeyPurposeId.id_kp_serverAuth));

        ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(keyPair.getPrivate());
        X509CertificateHolder holder = builder.build(signer);
        return new JcaX509CertificateConverter().getCertificate(holder);
    }

    private X509Certificates() {
        // private constructor to prevent instantiation
    }
}
