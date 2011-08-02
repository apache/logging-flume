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
package com.cloudera.flume.core;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;

import com.cloudera.flume.conf.Context;
import com.cloudera.flume.conf.SinkFactory.SinkDecoBuilder;
import com.google.common.base.Preconditions;

/**
 * This decorator calculates a digest of the event body and stores it into the
 * specified attribute. It can optionally base 64 encode the digest so that it
 * can readily be displayed in text form rather than raw bytes.
 */
public class DigestDecorator<S extends EventSink> extends EventSinkDecorator<S> {
  String attr;
  MessageDigest stomach;
  boolean encodeBase64;

  public DigestDecorator(S s, String algorithm, String attr, boolean encodeBase64) {
    super(s);
    this.attr = attr;
    this.encodeBase64 = encodeBase64;
    try {
      this.stomach = MessageDigest.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public synchronized void append(Event e) throws IOException, InterruptedException {
    this.stomach.reset();
    this.stomach.update(e.getBody());
    byte digest[] = this.stomach.digest();
    if (this.encodeBase64) {
      digest = Base64.encodeBase64(digest);
    }
    e.set(this.attr, digest);
    super.append(e);
  }

  public static SinkDecoBuilder builder() {
    return new SinkDecoBuilder() {
      @Override
      public EventSinkDecorator<EventSink> build(Context context,
          String... argv) {
        Preconditions.checkArgument(argv.length == 2,
            "usage: digest(algorithm, attr, [base64=boolean])");
        boolean encodeBase64 = false;
        String v = context.getValue("base64");
        if (v != null) {
          encodeBase64 = Boolean.parseBoolean(v);
        }
        return new DigestDecorator<EventSink>(null, argv[0], argv[1], encodeBase64);
      }
    };
  }
}
