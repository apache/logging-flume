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

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigRegistry;

/**
 * Dynamically add to the ApplicationContext.
 */
public class FlumeInitializer implements ApplicationContextInitializer {
  private Logger logger = LoggerFactory.getLogger(FlumeInitializer.class);

  @Override
  public void initialize(ConfigurableApplicationContext applicationContext) {
    if (applicationContext instanceof AnnotationConfigRegistry) {
      AnnotationConfigRegistry registry = (AnnotationConfigRegistry) applicationContext;
      ServiceLoader<PackageProvider> serviceLoader =
          ServiceLoader.load(PackageProvider.class, FlumeInitializer.class.getClassLoader());
      List<String> basePackages = new ArrayList<>();
      for (PackageProvider provider : serviceLoader) {
        basePackages.addAll(provider.getPackages());
      }
      logger.debug("Adding packages {} for component scanning", basePackages);
      registry.scan(basePackages.toArray(new String[0]));
    } else {
      logger.warn("ApplicationContext is not an AnnotationConfigRegistry. Application loading will likely fail");
    }
  }
}
