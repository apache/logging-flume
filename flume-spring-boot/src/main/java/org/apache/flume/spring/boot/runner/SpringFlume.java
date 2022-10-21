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
package org.apache.flume.spring.boot.runner;

import com.google.common.collect.Lists;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.node.Application;
import org.apache.flume.node.MaterializedConfiguration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;

/**
 *
 */
@Component
public class SpringFlume {
  private MaterializedConfiguration materializedConfiguration;

  private final Application application;

  @Autowired
  public SpringFlume(MaterializedConfiguration configuration) {
    this.materializedConfiguration = configuration;
    List<LifecycleAware> components = Lists.newArrayList();
    application = new Application(components);
  }

  @PostConstruct
  public void startUp() {
    application.start();
    application.handleConfigurationEvent(materializedConfiguration);
  }

  @PreDestroy
  public void shutdown() {
    application.stop();
  }


}
