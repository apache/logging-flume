/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.util;

/**
 * Some array helper methods not found in apache commons or guava.
 */
public final class ArrayUtils {
  private ArrayUtils() {
  }

  /**
   * Convert all arguments into strings by calling toString. Nulls are converted
   * to "".
   *
   * @param args
   * @return
   */
  public static String[] toStrings(Object... args) {
    String[] sargs = new String[args.length];
    for (int i = 0; i < args.length; i++) {
      if (args[i] == null) {
        sargs[i] = "";
      } else {
        sargs[i] = args[i].toString();
      }
    }
    return sargs;
  }

}
