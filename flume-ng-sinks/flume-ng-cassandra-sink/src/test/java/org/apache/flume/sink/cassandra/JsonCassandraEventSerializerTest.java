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

import org.apache.flume.Context;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

public class JsonCassandraEventSerializerTest {

  private JsonCassandraEventSerializer serializer;

  @Before
  public void before() {
    Context context = new Context();

    serializer = new JsonCassandraEventSerializer();
    serializer.configure(context);
  }

  @Test
  public void testGetActions() throws Exception {
    String event = "{\"consumed\":0,\"content\":\"Message [topic=T_OFFER_EDIT, flag=0, properties={TRACE_ID=7530281f-cacf-49cd-aaa8-5657e5ac2587, WAIT=true, TAGS=junping-test}, body=5669]\",\"msg_id\":\"AC1E1E7D00002A9F000000044C226B3C\",\"msg_topic\":\"T_OFFER_EDIT\",\"produced_at\":\"2017/05/10 04:58:21\",\"produced_host\":\"127.0.0.1\",\"system_env\":\"junping-test\",\"trace_id\":\"7530281f-cacf-49cd-aaa8-5657e5ac2587\"}";
    Map<String, Object> actions = serializer.getActions(event.getBytes());
    assertEquals(0, actions.get("consumed"));
  }
}