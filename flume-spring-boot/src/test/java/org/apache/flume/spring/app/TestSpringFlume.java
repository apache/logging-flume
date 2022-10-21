/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package org.apache.flume.spring.app;

import java.net.URI;

import org.apache.flume.node.MaterializedConfiguration;
import org.apache.flume.spring.boot.FlumeApplication;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 *
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = {FlumeApplication.class},
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class TestSpringFlume {

  @LocalServerPort
  private int port;

  @Autowired
  private TestRestTemplate restTemplate;

  @Autowired
  MaterializedConfiguration configuration;

  @Test
  public void contextLoads() {
    Assertions.assertThat(configuration).isNotNull();
    Assertions.assertThat(configuration.getSinkRunners()).isNotNull();
    Assertions.assertThat(configuration.getSinkRunners().size()).isEqualTo(1);
    String uri = "http://localhost:" + port + "/metrics";
    ResponseEntity<String> response = restTemplate.getForEntity(URI.create(uri), String.class);
    Assertions.assertThat(response).isNotNull();
    Assertions.assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
  }
}
