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
package org.apache.flume.sink.cassandra;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.conf.ComponentConfiguration;

import java.util.Map;

/**
 * The serializer which serializes the json format data into flume event.</p>
 * There must be the primary keys of target cassandra table in json input.</p>
 *
 * @see <code>JsonCassandraEventSerializerTest</code>
 *
 */
public class JsonCassandraEventSerializer implements CassandraEventSerializer {


  @Override
  public Map<String, Object> getActions(byte[] payload) {

    String event = new String(payload, Charsets.UTF_8);
    return (Map<String, Object>)JSONObject.parse(event);

  }

  @Override
  public void configure(Context context) {

  }

  @Override
  public void configure(ComponentConfiguration componentConfiguration) {

  }
}
