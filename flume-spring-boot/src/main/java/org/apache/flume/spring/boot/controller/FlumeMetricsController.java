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
package org.apache.flume.spring.boot.controller;

import java.lang.reflect.Type;
import java.util.Map;

import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * Retrieves Flume Metrics.
 */
@RestController
@ConditionalOnProperty(prefix = "flume", name = "metrics", havingValue = "http")
public class FlumeMetricsController {

  private final Type mapType = new MapTypeToken().getType();
  private final Gson gson = new Gson();

  @GetMapping(value = "/metrics", produces = "application/json;charset=utf-8")
  public String metrics() {
    Map<String, Map<String, String>> metricsMap = JMXPollUtil.getAllMBeans();
    return gson.toJson(metricsMap, mapType);
  }

  private static class MapTypeToken extends TypeToken<Map<String, Map<String, String>>> {
  }
}
