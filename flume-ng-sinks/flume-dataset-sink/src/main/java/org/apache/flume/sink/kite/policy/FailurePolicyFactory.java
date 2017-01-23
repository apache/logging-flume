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

package org.apache.flume.sink.kite.policy;

import java.util.Arrays;
import org.apache.flume.Context;

import static org.apache.flume.sink.kite.DatasetSinkConstants.*;

public class FailurePolicyFactory {

  public FailurePolicy newPolicy(Context config) {
    FailurePolicy policy;

    String policyType = config.getString(CONFIG_FAILURE_POLICY,
        DEFAULT_FAILURE_POLICY);

    if (policyType.equals(RETRY_FAILURE_POLICY)) {
      policy = new RetryPolicy.Builder().build(config);
    } else if (policyType.equals(SAVE_FAILURE_POLICY)) {
      policy = new SavePolicy.Builder().build(config);
    } else {

      Class<? extends FailurePolicy.Builder> builderClass;
      Class c;
      try {
        c = Class.forName(policyType);
      } catch (ClassNotFoundException ex) {
        throw new IllegalArgumentException("FailurePolicy.Builder class "
            + policyType + " not found. Must set " + CONFIG_FAILURE_POLICY
            + " to a class that implements FailurePolicy.Builder or to a builtin"
            + " policy: " + Arrays.toString(AVAILABLE_POLICIES), ex);
      }

      if (c != null && FailurePolicy.Builder.class.isAssignableFrom(c)) {
        builderClass = c;
      } else {
        throw new IllegalArgumentException("Class " + policyType + " does not"
            + " implement FailurePolicy.Builder. Must set "
            + CONFIG_FAILURE_POLICY + " to a class that extends"
            + " FailurePolicy.Builder or to a builtin policy: "
            + Arrays.toString(AVAILABLE_POLICIES));
      }

      FailurePolicy.Builder builder;
      try {
        builder = builderClass.newInstance();
      } catch (InstantiationException ex) {
        throw new IllegalArgumentException("Can't instantiate class "
            + policyType + ". Must set " + CONFIG_FAILURE_POLICY + " to a class"
            + " that extends FailurePolicy.Builder or to a builtin policy: "
            + Arrays.toString(AVAILABLE_POLICIES), ex);
      } catch (IllegalAccessException ex) {
        throw new IllegalArgumentException("Can't instantiate class "
            + policyType + ". Must set " + CONFIG_FAILURE_POLICY + " to a class"
            + " that extends FailurePolicy.Builder or to a builtin policy: "
            + Arrays.toString(AVAILABLE_POLICIES), ex);
      }

      policy = builder.build(config);
    }
   
    return policy;
  }
}
