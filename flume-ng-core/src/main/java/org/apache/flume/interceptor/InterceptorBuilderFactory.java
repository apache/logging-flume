/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.interceptor;

import org.apache.flume.interceptor.Interceptor.Builder;

/**
 * Factory used to register instances of Interceptors & their builders,
 * as well as to instantiate the builders.
 */
public class InterceptorBuilderFactory {

  private static Class<? extends Builder> lookup(String name) {
    try {
      return InterceptorType.valueOf(name.toUpperCase()).getBuilderClass();
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * Instantiate specified class, either alias or fully-qualified class name.
   */
  public static Builder newInstance(String name)
      throws ClassNotFoundException, InstantiationException,
      IllegalAccessException {

    Class<? extends Builder> clazz = lookup(name);
    if (clazz == null) {
      clazz = (Class<? extends Builder>) Class.forName(name);
    }
    return clazz.newInstance();
  }

}
