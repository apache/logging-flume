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

package org.apache.flume.channel.file;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Defines methods for reading from or writing to streams <p>
 *
 * Based on org.apache.hadoop.io.Writable
 */
interface Writable {
  /**
   * Serialize the fields of this object to <code>out</code>
   *
   * @param out <code>DataOutput</code> to serialize this object into.
   * @throws IOException
   */
  public void write(DataOutput out) throws IOException;

  /**
   * Deserialize the fields of this object from <code>in</code>
   *
   * @param in <code>DataInput</code> to deserialize this object from.
   * @throws IOException
   */
  public void readFields(DataInput in) throws IOException;
}
