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

package org.apache.flume.conf;

import org.apache.flume.Context;

/**
 * Methods for working with {@link Configurable}s.
 */
public class Configurables {

  /**
   * Check that {@code target} implements {@link Configurable} and, if so, ask
   * it to configure itself using the supplied {@code context}.
   *
   * @param target
   *          An object that potentially implements Configurable.
   * @param context
   *          The configuration context
   * @return true if {@code target} implements Configurable, false otherwise.
   */
  public static boolean configure(Object target, Context context) {
    if (target instanceof Configurable) {
      ((Configurable) target).configure(context);
      return true;
    }

    return false;
  }

  public static boolean configure(Object target, ComponentConfiguration conf) {
    if (target instanceof ConfigurableComponent) {
      ((ConfigurableComponent) target).configure(conf);
      return true;
    }
    return false;
  }

  public static void ensureRequiredNonNull(Context context, String... keys) {
    for (String key : keys) {
      if (!context.getParameters().containsKey(key)
          || context.getParameters().get(key) == null) {

        throw new IllegalArgumentException("Required parameter " + key
            + " must exist and may not be null");
      }
    }
  }

  public static void ensureOptionalNonNull(Context context, String... keys) {
    for (String key : keys) {
      if (context.getParameters().containsKey(key)
          && context.getParameters().get(key) == null) {

        throw new IllegalArgumentException("Optional parameter " + key
            + " may not be null");
      }
    }
  }

}
