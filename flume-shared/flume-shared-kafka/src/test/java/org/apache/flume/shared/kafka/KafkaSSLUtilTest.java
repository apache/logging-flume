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
package org.apache.flume.shared.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class KafkaSSLUtilTest {

  @Before
  public void initSystemProperties() {
    System.setProperty("javax.net.ssl.keyStore", "global-keystore-path");
    System.setProperty("javax.net.ssl.keyStorePassword", "global-keystore-password");
    System.setProperty("javax.net.ssl.keyStoreType", "global-keystore-type");
    System.setProperty("javax.net.ssl.trustStore", "global-truststore-path");
    System.setProperty("javax.net.ssl.trustStorePassword", "global-truststore-password");
    System.setProperty("javax.net.ssl.trustStoreType", "global-truststore-type");
  }

  @After
  public void clearSystemProperties() {
    System.clearProperty("javax.net.ssl.keyStore");
    System.clearProperty("javax.net.ssl.keyStorePassword");
    System.clearProperty("javax.net.ssl.keyStoreType");
    System.clearProperty("javax.net.ssl.trustStore");
    System.clearProperty("javax.net.ssl.trustStorePassword");
    System.clearProperty("javax.net.ssl.trustStoreType");
  }

  @Test
  public void testSecurityProtocol_PLAINTEXT() {
    Properties kafkaProps = new Properties();
    kafkaProps.put(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name);

    KafkaSSLUtil.addGlobalSSLParameters(kafkaProps);

    assertNoSSLParameters(kafkaProps);
  }

  @Test
  public void testSecurityProtocol_SASL_PLAINTEXT() {
    Properties kafkaProps = new Properties();
    kafkaProps.put(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_PLAINTEXT.name);

    KafkaSSLUtil.addGlobalSSLParameters(kafkaProps);

    assertNoSSLParameters(kafkaProps);
  }

  @Test
  public void testSecurityProtocol_SSL() {
    Properties kafkaProps = new Properties();
    kafkaProps.put(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);

    KafkaSSLUtil.addGlobalSSLParameters(kafkaProps);

    assertGlobalSSLParameters(kafkaProps);
  }

  @Test
  public void testSecurityProtocol_SASL_SSL() {
    Properties kafkaProps = new Properties();
    kafkaProps.put(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name);

    KafkaSSLUtil.addGlobalSSLParameters(kafkaProps);

    assertGlobalSSLParameters(kafkaProps);
  }

  @Test
  public void testComponentParametersNotOverridden() {
    Properties kafkaProps = new Properties();
    kafkaProps.put(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);

    kafkaProps.put(
        SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, "component-keystore-path");
    kafkaProps.put(
        SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, "component-keystore-password");
    kafkaProps.put(
        SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "component-keystore-type");
    kafkaProps.put(
        SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, "component-truststore-path");
    kafkaProps.put(
        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, "component-truststore-password");
    kafkaProps.put(
        SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "component-truststore-type");

    KafkaSSLUtil.addGlobalSSLParameters(kafkaProps);

    assertComponentSSLParameters(kafkaProps);
  }

  @Test
  public void testEmptyGlobalParametersNotAdded() {
    Properties kafkaProps = new Properties();
    kafkaProps.put(
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);

    clearSystemProperties();

    KafkaSSLUtil.addGlobalSSLParameters(kafkaProps);

    assertNoSSLParameters(kafkaProps);
  }

  private void assertNoSSLParameters(Properties kafkaProps) {
    assertFalse(kafkaProps.containsKey(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    assertFalse(kafkaProps.containsKey(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
    assertFalse(kafkaProps.containsKey(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG));
    assertFalse(kafkaProps.containsKey(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    assertFalse(kafkaProps.containsKey(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
    assertFalse(kafkaProps.containsKey(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
  }

  private void assertGlobalSSLParameters(Properties kafkaProps) {
    assertEquals("global-keystore-path",
        kafkaProps.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    assertEquals("global-keystore-password",
        kafkaProps.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
    assertEquals("global-keystore-type",
        kafkaProps.getProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG));
    assertEquals("global-truststore-path",
        kafkaProps.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    assertEquals("global-truststore-password",
        kafkaProps.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
    assertEquals("global-truststore-type",
        kafkaProps.getProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
  }

  private void assertComponentSSLParameters(Properties kafkaProps) {
    assertEquals("component-keystore-path",
        kafkaProps.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
    assertEquals("component-keystore-password",
        kafkaProps.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG));
    assertEquals("component-keystore-type",
        kafkaProps.getProperty(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG));
    assertEquals("component-truststore-path",
        kafkaProps.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG));
    assertEquals("component-truststore-password",
        kafkaProps.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG));
    assertEquals("component-truststore-type",
        kafkaProps.getProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG));
  }
}
