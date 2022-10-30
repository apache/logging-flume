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
package org.apache.flume.spring.boot.config;

import org.apache.flume.Channel;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;
import org.apache.flume.node.MaterializedConfiguration;
import org.apache.flume.node.SimpleMaterializedConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

/**
 *
 */
@Configuration
public class SpringConfiguration {

  @Autowired
  public Map<String, Channel> channels;

  @Autowired
  public Map<String, SinkRunner> sinkRunners;

  @Autowired
  public Map<String, SourceRunner> sourceRunners;

  @Bean
  public MaterializedConfiguration configuration() {
    MaterializedConfiguration config = new SimpleMaterializedConfiguration();
    for (Map.Entry<String, Channel> entry : channels.entrySet()) {
      config.addChannel(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, SinkRunner> entry : sinkRunners.entrySet()) {
      config.addSinkRunner(entry.getKey(), entry.getValue());
    }
    for (Map.Entry<String, SourceRunner> entry : sourceRunners.entrySet()) {
      config.addSourceRunner(entry.getKey(), entry.getValue());
    }
    return config;
  }
}
