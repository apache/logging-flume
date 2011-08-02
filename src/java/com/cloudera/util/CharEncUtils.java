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

import java.nio.charset.Charset;

public class CharEncUtils {
  final public static Charset RAW = Charset.forName("ISO-8859-1");
  final public static Charset LATIN1 = Charset.forName("ISO-8859-1");
  final public static Charset UTF8 = Charset.forName("UTF-8");

  public static byte[] toRaw(String s) {
    return s.getBytes(RAW);
  }
  
}
