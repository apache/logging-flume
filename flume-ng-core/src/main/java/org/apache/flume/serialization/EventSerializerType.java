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
package org.apache.flume.serialization;

import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public enum EventSerializerType {
  TEXT(BodyTextEventSerializer.Builder.class),
  HEADER_AND_TEXT(HeaderAndBodyTextEventSerializer.Builder.class),
  AVRO_EVENT("org.apache.flume.rpc.avro.FlumeEventAvroEventSerializer$Builder"),
  OTHER();

  private final Class<? extends EventSerializer.Builder> builderClass;

    EventSerializerType() {
        this.builderClass = null;
    }

  EventSerializerType(Class<? extends EventSerializer.Builder> builderClass) {
    this.builderClass = builderClass;
  }

    @SuppressWarnings("unchecked")
    EventSerializerType(String builderClassName) {
        Class<? extends EventSerializer.Builder> clazz;

        try {
            clazz = (Class<? extends EventSerializer.Builder>)Class.forName(builderClassName);
        } catch (ClassNotFoundException ex) {
            clazz = null;
        }
        this.builderClass = clazz;
    }

  public Class<? extends EventSerializer.Builder> getBuilderClass() {
    return builderClass;
  }

}
