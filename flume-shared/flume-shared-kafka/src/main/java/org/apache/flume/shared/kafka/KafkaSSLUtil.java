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

import org.apache.flume.util.SSLUtil;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.Properties;

public class KafkaSSLUtil {

  private KafkaSSLUtil() {
  }

  /**
   * Adds the global keystore/truststore SSL parameters to Kafka properties
   * if SSL is enabled but the keystore/truststore SSL parameters
   * are not defined explicitly in Kafka properties.
   *
   * @param kafkaProps Kafka properties
   */
  public static void addGlobalSSLParameters(Properties kafkaProps) {
    if (isSSLEnabled(kafkaProps)) {
      addGlobalSSLParameter(kafkaProps,
          SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, SSLUtil.getGlobalKeystorePath());
      addGlobalSSLParameter(kafkaProps,
          SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, SSLUtil.getGlobalKeystorePassword());
      addGlobalSSLParameter(kafkaProps,
          SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, SSLUtil.getGlobalKeystoreType(null));
      addGlobalSSLParameter(kafkaProps,
          SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, SSLUtil.getGlobalTruststorePath());
      addGlobalSSLParameter(kafkaProps,
          SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, SSLUtil.getGlobalTruststorePassword());
      addGlobalSSLParameter(kafkaProps,
          SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, SSLUtil.getGlobalTruststoreType(null));
    }
  }

  private static void addGlobalSSLParameter(Properties kafkaProps,
                                            String propName, String globalValue) {
    if (!kafkaProps.containsKey(propName) && globalValue != null) {
      kafkaProps.put(propName, globalValue);
    }
  }

  private static boolean isSSLEnabled(Properties kafkaProps) {
    String securityProtocol =
        kafkaProps.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);

    return securityProtocol != null &&
        (securityProtocol.equals(SecurityProtocol.SSL.name) ||
            securityProtocol.equals(SecurityProtocol.SASL_SSL.name));
  }

}
