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
package org.apache.flume.channel.file.encryption;

import org.apache.flume.FlumeException;

/**
 * Exception that is thrown when the channel is unable to decrypt an even
 * read from the channel.
 */
public class DecryptionFailureException extends FlumeException {
  private static final long serialVersionUID = 6646810195384793646L;


  public DecryptionFailureException(String msg) {
    super(msg);
  }

  public DecryptionFailureException(String msg, Throwable th) {
    super(msg, th);
  }
}
