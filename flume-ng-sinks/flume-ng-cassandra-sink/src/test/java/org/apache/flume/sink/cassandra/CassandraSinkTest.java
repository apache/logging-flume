package org.apache.flume.sink.cassandra;

import com.datastax.driver.core.Session;
import org.apache.flume.Context;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * Created by roger.lu on 17/5/11.
 */
public class CassandraSinkTest {

  private JsonCassandraEventSerializer serializer;
  private CassandraSink cassandraSink;
  private Session session;
  private Context context = new Context();

  @Before
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

  @Test
  public void testInsert() {
    String event = "{\"consumed_host\":\"ym-service-97997900-newhe\",\"msg_id\":\"AC1E1E7D00002A9F00000004FB3BE868\",\"msg_topic\":\"T_OFFER_PUBLISHER_EDIT\",\"system_env\":\"roger\",\"trace_id\":\"90\"}";
    Map<String, Object> actions = serializer.getActions(event.getBytes());
    session.execute(cassandraSink.getInsertOrUpdateStatement(actions));
  }

  @Test
  public void testUpdateCollection() {
    String event = "{\"consumed_at\":\"2017/05/27 01:11:28\",\"consumed_by\":\"OfferPublisherRegulationListener\",\"consumed_host\":\"ym-service-97997900-newhe\",\"consumed_system_env\":\"test-vision\",\"msg_id\":\"AC1E1E7D00002A9F00000004FB3BE868\",\"msg_topic\":\"T_OFFER_PUBLISHER_EDIT\",\"system_env\":\"roger\",\"trace_id\":\"90\"}";
    Map<String, Object> actions = serializer.getActions(event.getBytes());
    session.execute(cassandraSink.getInsertOrUpdateStatement(actions));
  }
}