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

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Update;
import org.apache.flume.Context;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by roger.lu on 17/5/11.
 */
public class CassandraSinkTest {

  private JsonCassandraEventSerializer serializer;
  private CassandraSink cassandraSink;
  private Session session;
  private Context context = new Context();

  public void before() {

    cassandraSink = new CassandraSink();
    context.put("serializer", "org.apache.flume.sink.cassandra.JsonCassandraEventSerializer");
    context.put("cassandra.contactPoints", "172.30.10.241");
    context.put("cassandra.username", "test");
    context.put("cassandra.password", "test");
    context.put("cassandra.keyspace", "ym_prod");
    context.put("cassandra.table", "message_trace");
    context.put("datetime.format", "yyyy/MM/dd HH:mm:ss");

    cassandraSink.configure(context);

    serializer = new JsonCassandraEventSerializer();
    serializer.configure(context);
    session = cassandraSink.getSession();
  }

  public void testInsert() {
    String event = "{\"consumed_host\":\"ym-service-97997900-newhe\"," +
        "\"msg_id\":\"AC1E1E7D00002A9F00000004FB3BE868\"," +
        "\"msg_topic\":\"T_OFFER_PUBLISHER_EDIT\",\"system_env\":\"roger\",\"trace_id\":\"90\"}";
    Map<String, Object> actions = serializer.getActions(event.getBytes());
    Update statement = cassandraSink.getInsertOrUpdateStatement(actions);
    assertEquals("UPDATE ym_prod.message_trace SET msg_topic='T_OFFER_PUBLISHER_EDIT' " +
        "WHERE trace_id='90' AND system_env='roger' AND msg_id='AC1E1E7D00002A9F00000004FB3BE868';",
        statement.toString());
    //session.execute(statement);
  }

  public void testUpdateCollection() {
    String event = "{\"consumed_at\":\"2017/05/27 01:11:28\"," +
        "\"consumed_by\":\"OfferPublisherRegulationListener\"," +
        "\"consumed_host\":\"ym-service-97997900-new\"," +
        "\"consumed_system_env\":\"test-vision\"," +
        "\"msg_id\":\"AC1E1E7D00002A9F00000004FB3BE868\"," +
        "\"msg_topic\":\"T_OFFER_PUBLISHER_EDIT\"," +
        "\"system_env\":\"roger\",\"trace_id\":\"90\"}";
    Map<String, Object> actions = serializer.getActions(event.getBytes());
    Update statement = cassandraSink.getInsertOrUpdateStatement(actions);
    assertEquals("UPDATE ym_prod.message_trace " +
        "SET consumed_by=consumed_by+{'OfferPublisherRegulationListener'}," +
        "msg_topic='T_OFFER_PUBLISHER_EDIT' " +
        "WHERE trace_id='90' AND system_env='roger' AND msg_id='AC1E1E7D00002A9F00000004FB3BE868';",
        statement.toString());
    //session.execute(statement);
  }
}