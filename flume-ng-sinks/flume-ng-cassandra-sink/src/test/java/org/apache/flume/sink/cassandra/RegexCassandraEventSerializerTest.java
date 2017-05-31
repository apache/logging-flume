package org.apache.flume.sink.cassandra;

import com.datastax.driver.core.TypeCodec;
import org.apache.flume.Context;
import org.junit.Before;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by roger.lu on 17/5/31.
 */
public class RegexCassandraEventSerializerTest {

  private  RegexCassandraEventSerializer serializer;

  @Before
  public void before() {

    Context context = new Context();

    context.put("regex", "[\\d\\.]+ - - (\\[.*\\]) \"[^ ]+ ([^ ?]+)\\?app_id=(\\d+)([^ ]+) [^ \"]+\" (\\d+) (\\d+) (\\d+)");
    context.put("colNames", "datetime,api,app_id,query_string,status_code,size,time_span");

    serializer = new RegexCassandraEventSerializer();
    serializer.configure(context);
  }

  @Test
  public void testGetActions() throws Exception {
    Map<String, Object> actions = serializer.getActions("172.30.91.67 - - [09/May/2017:02:55:58 +0000] \"GET /api?app_id=198&page=1&limit=20 HTTP/1.1\" 200 94534 3".getBytes());
    assertEquals("198", actions.get("app_id"));
    assertEquals("3", actions.get("time_span"));
  }

  @Test
  public void testFailSerialization() {
    Map<String, Object> actions = serializer.getActions("172.30.10.198 - - [09/May/2017:00:59:15 +0000] \"POST /api/search HTTP/1.1\" 404 949 36".getBytes());
    assertTrue(actions.isEmpty());
  }

}